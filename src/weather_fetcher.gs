// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STATION_ID = "IGREIF68";
const DATA_START_ROW = 2; // Row 1 is the header
const HEADER_ROW = 1;
const MAX_LOOKBACK_DAYS = 30; // Only fetch precipitation for this many days back

// Column header names used to find indices dynamically (robust to column reordering).
const DATE_HEADER = "Datum";
const PRECIP_HEADER = "Niederschlag_total_mm";

// ---------------------------------------------------------------------------
// Header / column resolution
// ---------------------------------------------------------------------------

/**
 * Reads the header row and returns 1-based column indices for date and precipitation.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @returns {{ dateColumn: number, precipColumn: number }}
 */
function getColumnIndicesFromHeader(sheet) {
  const lastCol = sheet.getLastColumn();
  if (lastCol < 1) {
    throw new Error("Sheet has no header row.");
  }
  const headerRow = sheet.getRange(HEADER_ROW, 1, HEADER_ROW, lastCol).getValues()[0];
  let dateColumn = null;
  let precipColumn = null;
  for (let c = 0; c < headerRow.length; c++) {
    const label = String(headerRow[c] || "").trim();
    if (label === DATE_HEADER) dateColumn = c + 1;
    if (label === PRECIP_HEADER) precipColumn = c + 1;
  }
  if (dateColumn == null) {
    throw new Error(`Header "${DATE_HEADER}" not found in row ${HEADER_ROW}. Check column order.`);
  }
  if (precipColumn == null) {
    throw new Error(`Header "${PRECIP_HEADER}" not found in row ${HEADER_ROW}. Check column order.`);
  }
  return { dateColumn, precipColumn };
}

// ---------------------------------------------------------------------------
// Main entry point — assign this function to your daily time trigger
// ---------------------------------------------------------------------------

function run() {
  Logger.log("=== run() started ===");

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("recordings");
  if (!sheet) {
    Logger.log('Sheet "recordings" not found — exiting.');
    return;
  }
  const lastRow = sheet.getLastRow();

  if (lastRow < DATA_START_ROW) {
    Logger.log("No data rows found — exiting.");
    return;
  }

  const { dateColumn, precipColumn } = getColumnIndicesFromHeader(sheet);
  Logger.log(`Using columns: ${DATE_HEADER}=${dateColumn}, ${PRECIP_HEADER}=${precipColumn}`);

  const rowCount     = lastRow - DATA_START_ROW + 1;
  const dateValues   = sheet.getRange(DATA_START_ROW, dateColumn,   rowCount, 1).getValues();
  const precipValues = sheet.getRange(DATA_START_ROW, precipColumn, rowCount, 1).getValues();

  // Yesterday as a YYYY-MM-DD string — we never fill data for today or future dates
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = Utilities.formatDate(yesterday, Session.getScriptTimeZone(), "yyyy-MM-dd");
  // Only consider data for the configured lookback window
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - MAX_LOOKBACK_DAYS);
  const cutoffStr = Utilities.formatDate(cutoff, Session.getScriptTimeZone(), "yyyy-MM-dd");
  Logger.log(`Considering dates from ${cutoffStr} to ${yesterdayStr}. Scanning ${rowCount} row(s)...`);

  // Collect rows that have a date but no precipitation value
  const missingRows = []; // [{ row, date }]

  for (let i = 0; i < rowCount; i++) {
    const cellValue = dateValues[i][0];
    const precip    = precipValues[i][0];

    if (!cellValue || precip !== "") continue;

    const dateStr = cellValue instanceof Date
      ? Utilities.formatDate(cellValue, Session.getScriptTimeZone(), "yyyy-MM-dd")
      : String(cellValue).substring(0, 10);

    // Skip future dates and dates older than cutoff days
    if (dateStr > yesterdayStr || dateStr < cutoffStr) continue;

    missingRows.push({ row: DATA_START_ROW + i, date: dateStr });
  }

  Logger.log(`${missingRows.length} row(s) missing precipitation data.`);

  if (missingRows.length === 0) {
    Logger.log("Nothing to do.");
    return;
  }

  missingRows.sort((a, b) => a.date.localeCompare(b.date));
  const startDate = missingRows[0].date;
  const endDate   = missingRows[missingRows.length - 1].date;
  Logger.log(`Fetching ${startDate} → ${endDate}`);

  const apiKey    = getApiKey();
  const precipMap = fetchPrecipitationMap(STATION_ID, startDate, endDate, apiKey);
  const precipDates = Object.keys(precipMap).sort();
  Logger.log(`API returned data for ${precipDates.length} day(s): ${precipDates.join(", ")}`);

  let updated = 0;
  for (const { row, date } of missingRows) {
    if (Object.prototype.hasOwnProperty.call(precipMap, date)) {
      sheet.getRange(row, precipColumn).setValue(precipMap[date]);
      updated++;
    } else {
      Logger.log(`  No API data for ${date} (row ${row}).`);
    }
  }

  Logger.log(`=== Done — updated ${updated} row(s). ===`);
}

// ---------------------------------------------------------------------------
// API key
// ---------------------------------------------------------------------------

function getApiKey() {
  const stored = PropertiesService.getScriptProperties().getProperty("WU_API_KEY");
  if (stored) {
    Logger.log("API key loaded from Script Properties.");
    return stored;
  }

  Logger.log("WU_API_KEY not in Script Properties — extracting from WUnderground page...");

  const url      = `https://www.wunderground.com/dashboard/pws/${STATION_ID}`;
  const response = UrlFetchApp.fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" },
    muteHttpExceptions: true,
  });

  Logger.log(`WUnderground page status: ${response.getResponseCode()}`);

  const html     = response.getContentText();
  const patterns = [
    /"apiKey"\s*:\s*"([a-f0-9]{32})"/,
    /apiKey=([a-f0-9]{32})/,
    /"key"\s*:\s*"([a-f0-9]{32})"/,
  ];

  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (match) {
      Logger.log("API key extracted from page source.");
      return match[1];
    }
  }

  throw new Error(
    "API key not found. Add WU_API_KEY to Script Properties: " +
    "Extensions → Apps Script → Project Settings → Script Properties."
  );
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

const CHUNK_DAYS = 30; // API rejects ranges larger than ~31 days

/**
 * Fetches precipitation totals for a date range and returns a map of
 * { "YYYY-MM-DD": precipTotal } covering all observations in that range.
 */
function fetchPrecipitationMap(stationId, startDate, endDate, apiKey) {
  const precipMap  = {};
  let   chunkStart = new Date(startDate);
  const end        = new Date(endDate);
  let   chunkIndex = 0;

  while (chunkStart <= end) {
    chunkIndex++;
    const chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + CHUNK_DAYS - 1);
    if (chunkEnd > end) chunkEnd.setTime(end.getTime());

    const startStr = Utilities.formatDate(chunkStart, "UTC", "yyyyMMdd");
    const endStr   = Utilities.formatDate(chunkEnd,   "UTC", "yyyyMMdd");

    Logger.log(`  Chunk ${chunkIndex}: ${startStr} → ${endStr}`);

    const url = "https://api.weather.com/v2/pws/history/daily"
      + `?stationId=${stationId}`
      + `&format=json`
      + `&units=m`
      + `&startDate=${startStr}`
      + `&endDate=${endStr}`
      + `&apiKey=${apiKey}`
      + `&numericPrecision=decimal`;

    const response = UrlFetchApp.fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" },
      muteHttpExceptions: true,
    });

    const statusCode = response.getResponseCode();
    if (statusCode !== 200) {
      Logger.log(`  Chunk ${chunkIndex}: status ${statusCode} — ${response.getContentText()}`);
      chunkStart.setDate(chunkStart.getDate() + CHUNK_DAYS);
      continue;
    }

    const observations = JSON.parse(response.getContentText()).observations || [];
    Logger.log(`  Chunk ${chunkIndex}: ${observations.length} observation(s) received.`);

    for (const obs of observations) {
      const obsDate = (obs.obsTimeLocal || obs.obsTimeUtc || "").substring(0, 10);
      const precip  = (obs.metric || {}).precipTotal;
      if (obsDate && precip != null) {
        precipMap[obsDate] = precip;
      }
    }

    chunkStart.setDate(chunkStart.getDate() + CHUNK_DAYS);
  }

  return precipMap;
}
