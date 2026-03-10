# Output Folder Guide

This folder contains generated files from `filter_runs_in_boundary.py`.

## Files

1. `Incident_Report_with_GIS.xlsx`
- Full combined dataset from all GIS CSV inputs
- Includes geocoded coordinates and geocode diagnostics

2. `runs_in_boundary.xlsx`
- Records whose coordinates fall inside the configured boundary polygon
- Main filtered result for the target geographic area

3. `Interstate.xlsx`
- Records where `address` matches configured interstate patterns
- Current pattern examples include `Mountain Pkwy` and `I 64 INST`

4. `unmatched_addresses.xlsx`
- Records that did not geocode successfully
- Use this file to review and correct addresses for reruns

5. `missing_incident_number_blocks.xlsx`
- Audit report showing large missing incident-number ranges
- Includes both overall and per-source-file gap checks

6. `geocode_cache.json`
- Address-to-coordinate cache used to speed up future runs
- Safe to keep between runs

## Key Columns

Common identifier fields:
- `cad_code` (text)
- `INCIDENT NUMBER` (text)

Both are intentionally stored as text to prevent Excel scientific notation.

Geocoding fields:
- `lat`, `lon`: output coordinates
- `coordinate_source`: `census`, `nominatim`, or `none`
- `geocode_status`: `matched`, `no_match`, `api_error`, or `blank`
- `geocode_input_used`: address string used for final geocode attempt

## Notes

- These files are regenerated each run and may be overwritten.
- If geocoded counts look low, review `unmatched_addresses.xlsx` first.
- If incident sequence quality matters, review `missing_incident_number_blocks.xlsx`.
