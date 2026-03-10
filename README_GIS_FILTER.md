# GIS Data Filter README

This document explains how `filter_runs_in_boundary.py` processes incident data in `GISDATA`, what each output file contains, and how to interpret key fields.

## What This Script Does

The script performs these steps in one run:

1. Reads all `.csv` files under `GISDATA` (recursive).
2. Normalizes incident identifiers and address fields.
3. Geocodes addresses using a fallback chain:
   - Primary: US Census Geocoder
   - Fallback: Nominatim (OpenStreetMap)
4. Applies boundary filtering for records inside `BOUNDARY_COORDS`.
5. Creates a subset for addresses matching interstate patterns.
6. Checks for large missing blocks in incident numbers.
7. Writes output workbooks to `output/`.

## Input Data Expected

Source folder:
- `GISDATA/`

Source files:
- Any CSV file (`*.csv`) in `GISDATA` and subfolders

Important source columns (if present):
- `INCIDENT NUMBER` (preferred for incident ID)
- `DISPATCH NUMBER` (fallback incident ID)
- `INCIDENT LOCATION` (preferred address)
- `DISPATCH LOCATION` (fallback address)

Normalized fields created by script:
- `cad_code`
- `address`
- `fallback_address`
- `source_file`

## Output Files

All outputs are written to `output/`:

1. `Incident_Report_with_GIS.xlsx`
- Full combined dataset from all GIS CSV files
- Includes geocoded `lat` and `lon`
- Includes geocode diagnostics (`geocode_status`, `coordinate_source`, `geocode_input_used`)

2. `runs_in_boundary.xlsx`
- Records whose coordinates fall inside `BOUNDARY_COORDS`
- Cleaned column set with `cad_code` first and `address` second

3. `Interstate.xlsx`
- Records where `address` contains interstate patterns
- Current patterns:
  - `mountain pkwy`
  - `i 64 inst`

4. `unmatched_addresses.xlsx`
- Records that did not geocode to a matched coordinate
- Used for manual cleanup and retry

5. `missing_incident_number_blocks.xlsx`
- Report of large missing incident-number ranges
- Includes overall (`all_files`) and per-source-file (`per_file`) scopes
- Large block threshold is controlled by `MIN_MISSING_BLOCK_SIZE`

6. `geocode_cache.json`
- Persistent geocode cache to speed up future runs
- Reuses prior address lookups

## Key Column Meanings

Geocoding columns:
- `lat`, `lon`: Final coordinates
- `coordinate_source`:
  - `census` - matched by US Census Geocoder
  - `nominatim` - matched by Nominatim fallback
  - `none` - no successful geocode
- `geocode_status`:
  - `matched` - coordinate found
  - `no_match` - geocoder returned no location
  - `api_error` - request error after retries
  - `blank` - no usable address in row
- `geocode_input_used`:
  - Address string actually used when geocoding succeeded/finalized

Identifier columns:
- `INCIDENT NUMBER` and `cad_code` are exported as text to prevent scientific notation in Excel.

## Missing Incident Number Block Report

`missing_incident_number_blocks.xlsx` columns:
- `scope`: `all_files` or `per_file`
- `source_file`: file name/path for per-file scope
- `previous_incident`: incident before the gap
- `next_incident`: incident after the gap
- `missing_start`: first missing incident number in the range
- `missing_end`: last missing incident number in the range
- `missing_count`: size of missing range

A gap is only reported when:
- `missing_count >= MIN_MISSING_BLOCK_SIZE`

Default threshold:
- `MIN_MISSING_BLOCK_SIZE = 25`

## Rate Limiting and API Behavior

Current geocoding pacing:
- Census: `CENSUS_MIN_INTERVAL_SECONDS = 0.25`
- Nominatim: `NOMINATIM_MIN_INTERVAL_SECONDS = 1.10`

Retries/backoff:
- Handles transient failures and throttling (`429`, `503`) with increasing delays.

## How To Run

From the project root:

```powershell
& ".venv/Scripts/python.exe" "filter_runs_in_boundary.py"
```

## Common Adjustments

If needed, edit these constants in `filter_runs_in_boundary.py`:
- `BOUNDARY_COORDS` - polygon for geographic filtering
- `INTERSTATE_PATTERNS` - text patterns for interstate file
- `MIN_MISSING_BLOCK_SIZE` - sensitivity of missing block checks
- `CENSUS_MIN_INTERVAL_SECONDS`, `NOMINATIM_MIN_INTERVAL_SECONDS` - API pacing

## Notes for Analysts

1. `runs_in_boundary.xlsx` is only as accurate as geocode quality for each address.
2. Always review `unmatched_addresses.xlsx` when counts look low.
3. If source CSVs overlap in time, duplicates may exist unless deduplicated downstream.
4. Use `missing_incident_number_blocks.xlsx` as an audit signal, not absolute proof of data loss.
