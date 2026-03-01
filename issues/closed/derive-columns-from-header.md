# Can we derive date and precipitation column indices from the header row?

Instead of hardcoding `DATE_COLUMN = 2` and `PRECIP_COLUMN = 6`, can we read the header row and find the correct column indices dynamically based on the column names? This would make the script more robust if columns are ever reordered.

## Resolution

Implemented in `src/weather_fetcher.gs`:

- Removed constants `DATE_COLUMN` and `PRECIP_COLUMN`.
- Added `getColumnIndicesFromHeader(sheet)` which reads row 1, finds the columns whose headers are `Datum` and `Niederschlag_Total_mm`, and returns their 1-based indices.
- `run()` now calls this helper and uses the returned `dateColumn` and `precipColumn` for all range reads and the precipitation write. If either header is missing, the script throws a clear error.

Column order is now derived from the sheet header, so reordering columns no longer breaks the script.
