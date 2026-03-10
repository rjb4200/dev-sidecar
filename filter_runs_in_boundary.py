from __future__ import annotations
import json
import re
from pathlib import Path
from time import monotonic, sleep
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


## User-provided boundary coordinates in (lat, lon) order.
#BOUNDARY_COORDS = [
#    (37.92977456211142, -84.08754224154511),
#    (37.90062299780809, -84.12219565664641),
#    (37.887411376180644, -84.12642695589447),
#    (37.86797613080906, -84.13206173381792),
#    (37.85241219225258, -84.08001123181819),
#    (37.81802679012433, -84.06902347288053),
#    (37.835525202610114, -84.00294136387771),
#    (37.92407190978642, -83.98268402667128),
#    (37.93295929566857, -83.96167518695809),
#    (37.96569454434089, -83.98079299771454),
#    (37.95432586348226, -84.05647354326852),
#]

BOUNDARY_COORDS = [
    (37.96579226922417, -84.20636811153997), 
    (37.94761722567091, -84.1912469115535), 
    (37.86607291603471, -84.20307836124232), 
    (37.897887722256485, -84.34871354220698), 
    (37.9926832600318, -84.3191711353169)  
]

INTERSTATE_PATTERNS = ["mountain pkwy", "i 64 inst"]

GEOCODER_BASE_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
DATA_DIR = Path("GISDATA")
GEOCODED_OUTPUT_NAME = "Incident_Report_with_GIS.xlsx"
GEOCODE_CACHE_NAME = "geocode_cache.json"
UNMATCHED_OUTPUT_NAME = "unmatched_addresses.xlsx"
MISSING_BLOCKS_OUTPUT_NAME = "missing_incident_number_blocks.xlsx"
MIN_MISSING_BLOCK_SIZE = 25

# Conservative request pacing to reduce provider throttling.
CENSUS_MIN_INTERVAL_SECONDS = 0.25
NOMINATIM_MIN_INTERVAL_SECONDS = 1.10

DROP_COLUMNS = {
    "id",
    "priority",
    "external_data",
    "place",
    "unit",
    "cross_street",
    "city",
    "state",
    "map_code",
    "map_id",
    "alert_key",
    "messages",
    "responses",
}

ADDRESS_REPLACEMENTS = {
    " MTN PKWY": " MOUNTAIN PKWY",
    " MOUNTAIN PARKWAY": " MOUNTAIN PKWY",
    " I-64": " I 64",
    "INTERSTATE 64": " I 64",
    " NB ": " NORTHBOUND ",
    " SB ": " SOUTHBOUND ",
    " EB ": " EASTBOUND ",
    " WB ": " WESTBOUND ",
}

_LAST_REQUEST_AT: dict[str, float] = {
    "census": 0.0,
    "nominatim": 0.0,
}


def enforce_rate_limit(provider: str) -> None:
    min_interval = (
        CENSUS_MIN_INTERVAL_SECONDS if provider == "census" else NOMINATIM_MIN_INTERVAL_SECONDS
    )
    elapsed = monotonic() - _LAST_REQUEST_AT.get(provider, 0.0)
    if elapsed < min_interval:
        sleep(min_interval - elapsed)
    _LAST_REQUEST_AT[provider] = monotonic()


def retry_backoff_seconds(attempt: int, error_code: int | None = None) -> float:
    if error_code in {429, 503}:
        return 2.0 * (attempt + 1)
    return 0.75 * (attempt + 1)


def point_in_polygon(lat: float, lon: float, polygon: Iterable[tuple[float, float]]) -> bool:
    """Return True when point is inside polygon using ray-casting."""
    vertices = list(polygon)
    inside = False
    j = len(vertices) - 1

    for i, (lat_i, lon_i) in enumerate(vertices):
        lat_j, lon_j = vertices[j]

        intersects = ((lon_i > lon) != (lon_j > lon)) and (
            lat < (lat_j - lat_i) * (lon - lon_i) / ((lon_j - lon_i) + 1e-15) + lat_i
        )
        if intersects:
            inside = not inside

        j = i

    return inside


def address_is_interstate(address: object) -> bool:
    if pd.isna(address):
        return False
    text = str(address).lower()
    return any(pattern in text for pattern in INTERSTATE_PATTERNS)


def normalize_address_for_geocode(address: object) -> str:
    """Normalize common dispatch shorthand into geocoder-friendly text."""
    if pd.isna(address):
        return ""

    text = str(address).upper().strip()
    text = text.replace("@", " AND ")

    # Remove unit/apartment details that can reduce geocode match rates.
    text = re.sub(r"\b(APT|UNIT|LOT|STE|SUITE)\b\s*[A-Z0-9-]*", "", text)
    text = re.sub(r"\s+", " ", text)

    for old, new in ADDRESS_REPLACEMENTS.items():
        text = text.replace(old, new)

    text = re.sub(r"\s+", " ", text).strip(" ,")
    return text


def geocode_address_census(address: str, timeout_seconds: int = 20) -> tuple[float | None, float | None, str]:
    """Geocode a single address via US Census geocoder API."""
    query_params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    request_url = f"{GEOCODER_BASE_URL}?{urlencode(query_params)}"
    request = Request(request_url, headers={"User-Agent": "GIS-Geo-Filter/1.0"})

    for attempt in range(4):
        try:
            enforce_rate_limit("census")
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except HTTPError as ex:
            if attempt == 3:
                return None, None, "api_error"
            sleep(retry_backoff_seconds(attempt, ex.code))
        except Exception:
            if attempt == 3:
                return None, None, "api_error"
            sleep(retry_backoff_seconds(attempt))

    matches = payload.get("result", {}).get("addressMatches", [])
    if not matches:
        return None, None, "no_match"

    coordinates = matches[0].get("coordinates", {})
    lon = coordinates.get("x")
    lat = coordinates.get("y")

    if lat is None or lon is None:
        return None, None, "no_match"

    return float(lat), float(lon), "matched"


def geocode_address_nominatim(address: str, timeout_seconds: int = 20) -> tuple[float | None, float | None, str]:
    """Geocode a single address via Nominatim fallback API."""
    query_params = {
        "q": address,
        "format": "jsonv2",
        "limit": 1,
    }
    request_url = f"{NOMINATIM_BASE_URL}?{urlencode(query_params)}"
    request = Request(request_url, headers={"User-Agent": "GIS-Geo-Filter/1.0 (contact: local-script)"})

    for attempt in range(3):
        try:
            enforce_rate_limit("nominatim")
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except HTTPError as ex:
            if attempt == 2:
                return None, None, "api_error"
            sleep(retry_backoff_seconds(attempt, ex.code))
        except (URLError, TimeoutError):
            if attempt == 2:
                return None, None, "api_error"
            sleep(retry_backoff_seconds(attempt))

    if not payload:
        return None, None, "no_match"

    match = payload[0]
    lat = match.get("lat")
    lon = match.get("lon")

    if lat is None or lon is None:
        return None, None, "no_match"

    return float(lat), float(lon), "matched"


def discover_csv_files(data_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in data_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".csv" and not path.name.startswith("~$")
    )


def build_geocoded_incident_dataframe(csv_path: Path) -> pd.DataFrame:
    """Load an incident CSV and normalize CAD/address columns."""
    df = pd.read_csv(csv_path)
    if df.empty:
        return df

    if "cad_code" not in df.columns:
        if "INCIDENT NUMBER" in df.columns:
            df["cad_code"] = df["INCIDENT NUMBER"]
        elif "DISPATCH NUMBER" in df.columns:
            df["cad_code"] = df["DISPATCH NUMBER"]

    if "address" not in df.columns:
        if "INCIDENT LOCATION" in df.columns:
            df["address"] = df["INCIDENT LOCATION"]
        elif "DISPATCH LOCATION" in df.columns:
            df["address"] = df["DISPATCH LOCATION"]

    if "address" not in df.columns:
        raise KeyError("No usable address column")

    if "DISPATCH LOCATION" in df.columns:
        df["fallback_address"] = df["DISPATCH LOCATION"]
    else:
        df["fallback_address"] = pd.NA

    df["address"] = df["address"].fillna("").astype(str).str.strip()
    df["fallback_address"] = df["fallback_address"].fillna("").astype(str).str.strip()
    return df


def load_all_incident_data(data_dir: Path, root_dir: Path) -> pd.DataFrame:
    csv_files = discover_csv_files(data_dir)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    frames: list[pd.DataFrame] = []

    for csv_file in csv_files:
        try:
            df = build_geocoded_incident_dataframe(csv_file)
        except Exception as ex:
            print(f"Skipping {csv_file.name}: {ex}")
            continue

        if df.empty:
            print(f"Skipping {csv_file.name}: empty file")
            continue

        source_rel = csv_file.relative_to(root_dir).as_posix()
        df.insert(0, "source_file", source_rel)
        frames.append(df)
        print(f"Loaded {csv_file.name}: rows={len(df)}")

    if not frames:
        raise ValueError("No usable incident CSV files found in GISDATA")

    return pd.concat(frames, ignore_index=True)


def load_cache(cache_path: Path) -> dict[str, tuple[float | None, float | None, str, str]]:
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return {
            key: (
                value.get("lat"),
                value.get("lon"),
                value.get("status", "api_error"),
                value.get("provider", "unknown"),
            )
            for key, value in payload.items()
        }
    except Exception:
        return {}


def save_cache(cache_path: Path, cache_data: dict[str, tuple[float | None, float | None, str, str]]) -> None:
    serializable = {
        key: {"lat": lat, "lon": lon, "status": status, "provider": provider}
        for key, (lat, lon, status, provider) in cache_data.items()
    }
    cache_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")


def geocode_candidate_with_chain(
    candidate: str,
    address_cache: dict[str, tuple[float | None, float | None, str, str]],
) -> tuple[float | None, float | None, str, str]:
    """Try Census first, then Nominatim, caching by normalized address."""
    if not candidate:
        return None, None, "blank", "none"

    cache_key = normalize_address_for_geocode(candidate)
    if not cache_key:
        return None, None, "blank", "none"

    if cache_key in address_cache:
        return address_cache[cache_key]

    lat, lon, status = geocode_address_census(cache_key)
    if status == "matched":
        result = (lat, lon, "matched", "census")
        address_cache[cache_key] = result
        return result

    lat, lon, status = geocode_address_nominatim(cache_key)
    if status == "matched":
        result = (lat, lon, "matched", "nominatim")
        address_cache[cache_key] = result
        return result

    result = (None, None, status, "none")
    address_cache[cache_key] = result
    return result


def geocode_with_cache(df: pd.DataFrame, cache_path: Path) -> pd.DataFrame:
    address_cache = load_cache(cache_path)

    latitudes: list[float | None] = []
    longitudes: list[float | None] = []
    geocode_status: list[str] = []
    geocode_provider: list[str] = []
    geocode_input_used: list[str] = []

    total_rows = len(df)
    new_lookups = 0

    for idx, row in df.iterrows():
        primary_address = str(row.get("address", "") or "").strip()
        fallback_address = str(row.get("fallback_address", "") or "").strip()

        if not primary_address and not fallback_address:
            latitudes.append(None)
            longitudes.append(None)
            geocode_status.append("blank")
            geocode_provider.append("none")
            geocode_input_used.append("")
            continue

        cache_size_before = len(address_cache)
        lat, lon, status, provider = geocode_candidate_with_chain(primary_address, address_cache)
        input_used = primary_address

        if status != "matched" and fallback_address and fallback_address != primary_address:
            lat, lon, status, provider = geocode_candidate_with_chain(fallback_address, address_cache)
            input_used = fallback_address if status == "matched" else input_used

        if len(address_cache) > cache_size_before:
            new_lookups += 1
            # Keep API requests polite and reduce burst failures.
            sleep(0.03)

        latitudes.append(lat)
        longitudes.append(lon)
        geocode_status.append(status)
        geocode_provider.append(provider)
        geocode_input_used.append(input_used)

        if (idx + 1) % 200 == 0 or (idx + 1) == total_rows:
            print(f"Geocoding progress: {idx + 1}/{total_rows} incident rows")
            save_cache(cache_path, address_cache)

    save_cache(cache_path, address_cache)

    if new_lookups == 0:
        print(f"Geocoding cache hit for all {total_rows} incident rows")
    else:
        print(f"Geocoded {new_lookups} new lookups; cache size is now {len(address_cache)}")

    enriched = df.copy()
    enriched["lat"] = latitudes
    enriched["lon"] = longitudes
    enriched["coordinate_source"] = geocode_provider
    enriched["geocode_status"] = geocode_status
    enriched["geocode_input_used"] = geocode_input_used
    return enriched


def find_large_missing_blocks(series: pd.Series, min_missing_size: int) -> pd.DataFrame:
    numeric = pd.to_numeric(series, errors="coerce").dropna().astype("int64")
    unique_sorted = sorted(numeric.unique())

    rows: list[dict[str, int]] = []
    if len(unique_sorted) < 2:
        return pd.DataFrame(columns=["previous_incident", "next_incident", "missing_start", "missing_end", "missing_count"])

    for previous_incident, next_incident in zip(unique_sorted, unique_sorted[1:]):
        gap = int(next_incident - previous_incident - 1)
        if gap >= min_missing_size:
            rows.append(
                {
                    "previous_incident": int(previous_incident),
                    "next_incident": int(next_incident),
                    "missing_start": int(previous_incident + 1),
                    "missing_end": int(next_incident - 1),
                    "missing_count": int(gap),
                }
            )

    return pd.DataFrame(rows)


def build_missing_block_report(df: pd.DataFrame, min_missing_size: int) -> pd.DataFrame:
    report_frames: list[pd.DataFrame] = []

    overall = find_large_missing_blocks(df.get("cad_code", pd.Series(dtype="float64")), min_missing_size)
    if not overall.empty:
        overall.insert(0, "scope", "all_files")
        overall.insert(1, "source_file", "ALL")
        report_frames.append(overall)

    for source_file, source_df in df.groupby("source_file", dropna=False):
        gaps = find_large_missing_blocks(source_df.get("cad_code", pd.Series(dtype="float64")), min_missing_size)
        if gaps.empty:
            continue
        gaps.insert(0, "scope", "per_file")
        gaps.insert(1, "source_file", str(source_file))
        report_frames.append(gaps)

    if not report_frames:
        return pd.DataFrame(
            columns=[
                "scope",
                "source_file",
                "previous_incident",
                "next_incident",
                "missing_start",
                "missing_end",
                "missing_count",
            ]
        )

    return pd.concat(report_frames, ignore_index=True).sort_values(
        by=["missing_count", "scope", "source_file"],
        ascending=[False, True, True],
    )


def coerce_identifier_columns_to_text(df: pd.DataFrame) -> pd.DataFrame:
    """Store identifier columns as text so Excel does not use scientific notation."""
    formatted = df.copy()
    for column in ["cad_code", "INCIDENT NUMBER"]:
        if column not in formatted.columns:
            continue

        numeric = pd.to_numeric(formatted[column], errors="coerce")
        as_text = formatted[column].astype("string").str.strip()
        as_text = as_text.mask(numeric.notna(), numeric.astype("Int64").astype("string"))
        formatted[column] = as_text.fillna("")

    return formatted


def finalize_output_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Apply final column cleanup and ordering rules for exported files."""
    cleaned = coerce_identifier_columns_to_text(df)

    cleaned = cleaned.drop(columns=[col for col in DROP_COLUMNS if col in cleaned.columns])

    ordered_columns = []
    if "cad_code" in cleaned.columns:
        ordered_columns.append("cad_code")
    if "address" in cleaned.columns:
        ordered_columns.append("address")

    ordered_columns += [col for col in cleaned.columns if col not in {"cad_code", "address"}]

    return cleaned[ordered_columns]


def main() -> None:
    root_dir = Path(__file__).resolve().parent
    data_dir = root_dir / DATA_DIR
    output_dir = root_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.exists():
        raise FileNotFoundError(f"GISDATA folder not found: {data_dir}")

    incident_df = load_all_incident_data(data_dir, root_dir)
    print(
        f"Loaded total incident rows: {len(incident_df)} from {incident_df['source_file'].nunique()} GISDATA files"
    )
    print(
        "Rate limit settings: "
        f"Census {CENSUS_MIN_INTERVAL_SECONDS:.2f}s/request, "
        f"Nominatim {NOMINATIM_MIN_INTERVAL_SECONDS:.2f}s/request"
    )

    missing_blocks_output_path = output_dir / MISSING_BLOCKS_OUTPUT_NAME
    missing_block_report = build_missing_block_report(incident_df, MIN_MISSING_BLOCK_SIZE)
    if missing_block_report.empty:
        missing_block_report.to_excel(missing_blocks_output_path, index=False)
        print(
            f"No large INCIDENT NUMBER gaps found (threshold={MIN_MISSING_BLOCK_SIZE}). Report: {missing_blocks_output_path}"
        )
    else:
        missing_block_report.to_excel(missing_blocks_output_path, index=False)
        print(
            f"Found {len(missing_block_report)} large INCIDENT NUMBER gaps (threshold={MIN_MISSING_BLOCK_SIZE}). "
            f"Report: {missing_blocks_output_path}"
        )

    cache_path = output_dir / GEOCODE_CACHE_NAME
    incident_df = geocode_with_cache(incident_df, cache_path)
    incident_df = coerce_identifier_columns_to_text(incident_df)

    geocoded_output_path = output_dir / GEOCODED_OUTPUT_NAME
    incident_df.to_excel(geocoded_output_path, index=False)

    print(
        f"Geocoded incident rows: total={len(incident_df)}, matched={int((incident_df['geocode_status'] == 'matched').sum())}"
    )
    print(f"Saved geocoded file to: {geocoded_output_path}")

    unmatched_output_path = output_dir / UNMATCHED_OUTPUT_NAME
    unmatched = incident_df[incident_df["geocode_status"] != "matched"].copy()
    if not unmatched.empty:
        keep_cols = [
            col
            for col in ["cad_code", "address", "fallback_address", "geocode_status", "geocode_input_used", "source_file"]
            if col in unmatched.columns
        ]
        unmatched_export = coerce_identifier_columns_to_text(unmatched[keep_cols])
        unmatched_export.to_excel(unmatched_output_path, index=False)
        print(f"Saved {len(unmatched)} unresolved rows to: {unmatched_output_path}")
    else:
        pd.DataFrame(
            columns=["cad_code", "address", "fallback_address", "geocode_status", "geocode_input_used", "source_file"]
        ).to_excel(unmatched_output_path, index=False)
        print(f"No unresolved geocodes. Created empty file: {unmatched_output_path}")

    boundary_output_path = output_dir / "runs_in_boundary.xlsx"
    interstate_output_path = output_dir / "Interstate.xlsx"

    if "lat" in incident_df.columns and "lon" in incident_df.columns:
        working = incident_df.copy()
        working["lat"] = pd.to_numeric(working["lat"], errors="coerce")
        working["lon"] = pd.to_numeric(working["lon"], errors="coerce")
        working = working.dropna(subset=["lat", "lon"])

        inside_mask = working.apply(
            lambda row: point_in_polygon(float(row["lat"]), float(row["lon"]), BOUNDARY_COORDS),
            axis=1,
        )
        boundary_result = working.loc[inside_mask].copy()

        print(f"Boundary filter: total={len(incident_df)}, with_coords={len(working)}, inside={len(boundary_result)}")

        if not boundary_result.empty:
            boundary_result = finalize_output_frame(boundary_result)
            boundary_result.to_excel(boundary_output_path, index=False)
            print(f"Saved {len(boundary_result)} rows to: {boundary_output_path}")
        else:
            pd.DataFrame(columns=["source_file"]).to_excel(boundary_output_path, index=False)
            print(f"No rows were inside the boundary. Created empty file: {boundary_output_path}")
    else:
        pd.DataFrame(columns=["source_file"]).to_excel(boundary_output_path, index=False)
        print("Skipping boundary filter: geocoded dataset is missing 'lat'/'lon' columns")

    if "address" in incident_df.columns:
        interstate_result = incident_df[incident_df["address"].apply(address_is_interstate)].copy()
        if not interstate_result.empty:
            interstate_result = finalize_output_frame(interstate_result)
            interstate_result.to_excel(interstate_output_path, index=False)
            print(f"Saved {len(interstate_result)} rows to: {interstate_output_path}")
        else:
            pd.DataFrame(columns=["source_file", "address"]).to_excel(interstate_output_path, index=False)
            print(f"No interstate address rows found. Created empty file: {interstate_output_path}")


if __name__ == "__main__":
    main()
