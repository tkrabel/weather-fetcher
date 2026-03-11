# TemperatureFetcher — Design Spec

**Date:** 2026-03-11
**Status:** Approved

## Overview

A Google Apps Script (`TemperatureFetcher.gs`) that runs on a Google Sheet named **Temperaturdaten**. It backfills missing date rows and fetches average daily temperature from the Weather Underground PWS API for each missing date.

## Configuration

| Constant | Value |
|---|---|
| `STATION_ID` | `"IGREIF68"` |
| `SHEET_NAME` | `"Temperaturdaten"` |
| `DATE_HEADER` | `"Datum"` |
| `TEMP_HEADER` | `"Durchschnittstemp [°C]"` |
| `MAX_LOOKBACK_DAYS` | `30` |
| `CHUNK_DAYS` | `30` |

## Header Row Detection

Scan from row 1 downward (up to 20 rows as safety limit) until finding a row that contains both `"Datum"` and `"Durchschnittstemp [°C]"`. Returns `{ headerRow, dataStartRow, dateColumn, tempColumn }`. Throws a descriptive error if headers are not found.

This makes the script robust to the header row being moved (currently row 4, but may shift to row 5 or 6 later).

## Phase 1 — Date Gap Filling

1. Read the Datum column from `dataStartRow` to last row.
2. Find the last date value in that column.
3. Compute `yesterday` and `cutoff = yesterday − MAX_LOOKBACK_DAYS days`.
4. If `lastDate >= yesterday` → nothing to append, skip phase.
5. For each date from `lastDate + 1` to `yesterday`:
   - Skip dates older than `cutoff`.
   - Append a new row with a JS `Date` object in `dateColumn`; leave `tempColumn` empty.
6. Rows are appended in chronological order. Date cells use actual Date values so Google Sheets formats them natively.

## Phase 2 — Temperature Fill

1. Re-read all data rows (including newly appended ones from Phase 1).
2. Collect rows where `Datum` is set but `Durchschnittstemp [°C]` is empty, and the date falls within the 30-day lookback window.
3. Sort missing rows by date; determine `startDate` → `endDate` range.
4. Fetch from the Weather Underground PWS Daily History API in `CHUNK_DAYS`-sized chunks:
   - Endpoint: `GET https://api.weather.com/v2/pws/history/daily`
   - Params: `stationId`, `startDate`, `endDate`, `format=json`, `units=m`, `numericPrecision=decimal`, `apiKey`
   - Extract `obs.metric.tempAvg` per observation date (`obsTimeLocal` or `obsTimeUtc`).
5. Write temperature values back to the corresponding sheet rows.

## API Key

Loaded from Script Properties (`WU_API_KEY`). If not set, falls back to extracting the key from the WUnderground station dashboard page (same pattern as `rain_fetcher.gs`).

## Entry Point

`function run()` — assign to a daily time-based trigger in Apps Script.

## Error Handling

- Sheet not found → log and return early.
- Headers not found → throw descriptive error.
- API non-200 response → log and skip that chunk, continue.
- No data for a specific date → log, leave cell empty.

## File Location

`src/TemperatureFetcher.gs`
