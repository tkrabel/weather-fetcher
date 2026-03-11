// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STATION_ID = "IGREIF68";
const HISTORY_START_DATE = "2024-01-01"; // First date to fetch when the sheet has no data
const CHUNK_DAYS = 30;                   // API rejects ranges larger than ~31 days
const DATE_HEADER = "Datum";

// ---------------------------------------------------------------------------
// Main entry point — assign this function to your daily time trigger
// ---------------------------------------------------------------------------

function run() {
  Logger.log("=== run() started ===");

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();

  const { headerRow, columnMap } = findHeaderRow(sheet);
  if (!headerRow) {
    Logger.log('Header row with "Datum" column not found — exiting.');
    return;
  }
  Logger.log(`Header found at row ${headerRow}. Columns: ${Object.keys(columnMap).join(", ")}`);

  const dataStartRow = headerRow + 1;
  const dateCol = columnMap[DATE_HEADER];

  // Determine date range
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = Utilities.formatDate(yesterday, Session.getScriptTimeZone(), "yyyy-MM-dd");

  const lastDateStr = getLastDate(sheet, dataStartRow, dateCol);
  Logger.log(`Last date in sheet: ${lastDateStr || "none"}`);

  let startStr;
  if (lastDateStr) {
    const nextDay = new Date(lastDateStr);
    nextDay.setDate(nextDay.getDate() + 1);
    startStr = Utilities.formatDate(nextDay, Session.getScriptTimeZone(), "yyyy-MM-dd");
  } else {
    startStr = HISTORY_START_DATE;
  }

  if (startStr > yesterdayStr) {
    Logger.log("Data is already up to date — nothing to do.");
    return;
  }

  Logger.log(`Fetching ${startStr} → ${yesterdayStr}`);

  const apiKey = getApiKey();
  const observations = fetchObservations(STATION_ID, startStr, yesterdayStr, apiKey);
  Logger.log(`${observations.length} observation(s) received total.`);

  // Sort by date and append new rows
  observations.sort((a, b) => {
    const da = (a.obsTimeLocal || a.obsTimeUtc || "").substring(0, 10);
    const db = (b.obsTimeLocal || b.obsTimeUtc || "").substring(0, 10);
    return da.localeCompare(db);
  });

  const totalCols = sheet.getLastColumn();
  let added = 0;

  for (const obs of observations) {
    const obsDate = (obs.obsTimeLocal || obs.obsTimeUtc || "").substring(0, 10);
    if (!obsDate) continue;

    const row = buildRow(obs, columnMap, totalCols);
    sheet.appendRow(row);
    added++;
  }

  Logger.log(`=== Done — added ${added} row(s). ===`);
}

// ---------------------------------------------------------------------------
// Header / column resolution
// ---------------------------------------------------------------------------

/**
 * Scans the sheet from row 1 downward (up to 20 rows) looking for a row
 * that contains the DATE_HEADER column. Returns the header row number and
 * a map of { columnName: 1-based column index }.
 */
function findHeaderRow(sheet) {
  const maxRows = Math.min(sheet.getLastRow(), 20);
  const lastCol = sheet.getLastColumn();
  if (maxRows < 1 || lastCol < 1) return { headerRow: null, columnMap: {} };

  const data = sheet.getRange(1, 1, maxRows, lastCol).getValues();

  for (let r = 0; r < data.length; r++) {
    const columnMap = {};
    for (let c = 0; c < data[r].length; c++) {
      const label = String(data[r][c] || "").trim();
      if (label) columnMap[label] = c + 1;
    }
    if (columnMap[DATE_HEADER]) {
      return { headerRow: r + 1, columnMap };
    }
  }

  return { headerRow: null, columnMap: {} };
}

// ---------------------------------------------------------------------------
// Last date
// ---------------------------------------------------------------------------

/**
 * Returns the date string from the last populated row's date column, or null if no data rows.
 */
function getLastDate(sheet, dataStartRow, dateCol) {
  const lastRow = sheet.getLastRow();
  if (lastRow < dataStartRow) return null;

  const val = sheet.getRange(lastRow, dateCol).getValue();
  if (!val) return null;
  return Utilities.formatDate(new Date(val), Session.getScriptTimeZone(), "yyyy-MM-dd");
}

// ---------------------------------------------------------------------------
// Row builder — maps observation fields to sheet columns by header name
// ---------------------------------------------------------------------------

const COLUMN_EXTRACTORS = {
  "Datum":                         obs => { const d = (obs.obsTimeLocal || obs.obsTimeUtc || "").substring(0, 10); return d ? new Date(d + "T12:00:00") : ""; },
  "Temp_Hoch_°C":                  obs => obs.metric?.tempHigh    ?? "",
  "Temp_Avg_°C":                   obs => obs.metric?.tempAvg     ?? "",
  "Temp_Tief_°C":                  obs => obs.metric?.tempLow     ?? "",
  "Taupunkt_Hoch_°C":              obs => obs.metric?.dewptHigh   ?? "",
  "Taupunkt_Avg_°C":               obs => obs.metric?.dewptAvg    ?? "",
  "Taupunkt_Tief_°C":              obs => obs.metric?.dewptLow    ?? "",
  "Windchill_Hoch_°C":             obs => obs.metric?.windchillHigh ?? "",
  "Windchill_Avg_°C":              obs => obs.metric?.windchillAvg  ?? "",
  "Windchill_Tief_°C":             obs => obs.metric?.windchillLow  ?? "",
  "Hitzeindex_Hoch_°C":            obs => obs.metric?.heatindexHigh ?? "",
  "Hitzeindex_Avg_°C":             obs => obs.metric?.heatindexAvg  ?? "",
  "Hitzeindex_Tief_°C":            obs => obs.metric?.heatindexLow  ?? "",
  "Luftfeuchtigkeit_Hoch_%":       obs => obs.humidityHigh ?? "",
  "Luftfeuchtigkeit_Avg_%":        obs => obs.humidityAvg  ?? "",
  "Luftfeuchtigkeit_Tief_%":       obs => obs.humidityLow  ?? "",
  "Windgeschwindigkeit_Hoch_km/h": obs => obs.metric?.windspeedHigh ?? "",
  "Windgeschwindigkeit_Avg_km/h":  obs => obs.metric?.windspeedAvg  ?? "",
  "Windgeschwindigkeit_Tief_km/h": obs => obs.metric?.windspeedLow  ?? "",
  "Windböe_Hoch_km/h":             obs => obs.metric?.windgustHigh  ?? "",
  "Windböe_Avg_km/h":              obs => obs.metric?.windgustAvg   ?? "",
  "Windböe_Tief_km/h":             obs => obs.metric?.windgustLow   ?? "",
  "Windrichtung_Avg_°":            obs => obs.winddirAvg          ?? "",
  "Luftdruck_Max_hPa":             obs => obs.metric?.pressureMax   ?? "",
  "Luftdruck_Min_hPa":             obs => obs.metric?.pressureMin   ?? "",
  "Luftdruck_Trend_hPa":           obs => obs.metric?.pressureTrend ?? "",
  "Sonnenstrahlung_Hoch_W/m2":     obs => obs.solarRadiationHigh  ?? "",
  "UV_Index_Hoch":                 obs => obs.uvHigh               ?? "",
  "Niederschlag_Total_mm":         obs => obs.metric?.precipTotal  ?? "",
  "Niederschlagsrate_Max_mm/h":    obs => obs.metric?.precipRate   ?? "",
  "QC_Status":                     obs => obs.qcStatus             ?? "",
};

function buildRow(obs, columnMap, totalCols) {
  const row = new Array(totalCols).fill("");
  for (const [header, extractor] of Object.entries(COLUMN_EXTRACTORS)) {
    const col = columnMap[header];
    if (col) row[col - 1] = extractor(obs);
  }
  return row;
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

  const url = `https://www.wunderground.com/dashboard/pws/${STATION_ID}`;
  const response = UrlFetchApp.fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" },
    muteHttpExceptions: true,
  });

  const html = response.getContentText();
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

function fetchObservations(stationId, startDate, endDate, apiKey) {
  const allObservations = [];
  let chunkStart = new Date(startDate);
  const end = new Date(endDate);
  let chunkIndex = 0;

  while (chunkStart <= end) {
    chunkIndex++;
    const chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + CHUNK_DAYS - 1);
    if (chunkEnd > end) chunkEnd.setTime(end.getTime());

    const tz = Session.getScriptTimeZone();
    const startStr = Utilities.formatDate(chunkStart, tz, "yyyyMMdd");
    const endStr   = Utilities.formatDate(chunkEnd,   tz, "yyyyMMdd");

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
    allObservations.push(...observations);

    chunkStart.setDate(chunkStart.getDate() + CHUNK_DAYS);
  }

  return allObservations;
}
