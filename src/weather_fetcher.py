"""
weather_fetcher.py
------------------
Cron-job script that fetches daily weather summaries for PWS IGREIF68
from the weather.com API and appends new days to data/wetterdaten.csv.

Run daily, e.g.:
    0 6 * * * /usr/bin/python3 /path/to/src/weather_fetcher.py

On each run the script fetches all days from the day after the last
recorded date up to yesterday, so it self-heals regardless of how long
the PC was offline.  If the CSV is empty, it starts from HISTORY_START_DATE.
"""

import csv
import os
import re
from datetime import date, timedelta

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STATION_ID = "IGREIF68"

# First date to fetch when the CSV doesn't exist yet.
HISTORY_START_DATE = date(2024, 1, 1)

_HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(_HERE, "..", "data", "wetterdaten.csv")

CSV_HEADERS = [
    "Datum",
    # Temperature
    "Temp_Hoch_°C",
    "Temp_Avg_°C",
    "Temp_Tief_°C",
    # Dew point
    "Taupunkt_Hoch_°C",
    "Taupunkt_Avg_°C",
    "Taupunkt_Tief_°C",
    # Wind chill
    "Windchill_Hoch_°C",
    "Windchill_Avg_°C",
    "Windchill_Tief_°C",
    # Heat index
    "Hitzeindex_Hoch_°C",
    "Hitzeindex_Avg_°C",
    "Hitzeindex_Tief_°C",
    # Humidity
    "Luftfeuchtigkeit_Hoch_%",
    "Luftfeuchtigkeit_Avg_%",
    "Luftfeuchtigkeit_Tief_%",
    # Wind speed
    "Windgeschwindigkeit_Hoch_km/h",
    "Windgeschwindigkeit_Avg_km/h",
    "Windgeschwindigkeit_Tief_km/h",
    # Wind gusts
    "Windböe_Hoch_km/h",
    "Windböe_Avg_km/h",
    "Windböe_Tief_km/h",
    # Wind direction
    "Windrichtung_Avg_°",
    # Pressure
    "Luftdruck_Max_hPa",
    "Luftdruck_Min_hPa",
    "Luftdruck_Trend_hPa",
    # Solar / UV
    "Sonnenstrahlung_Hoch_W/m2",
    "UV_Index_Hoch",
    # Precipitation
    "Niederschlag_Total_mm",
    "Niederschlagsrate_Max_mm/h",
    # Quality control
    "QC_Status",
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def get_fetch_date_range(csv_path: str) -> tuple[date, date] | None:
    """Return (start, yesterday) covering all days not yet in the CSV.

    start is the day after the last recorded date, or HISTORY_START_DATE if
    the CSV is empty.  Returns None if the CSV is already up to date.
    """
    yesterday = date.today() - timedelta(days=1)
    existing_dates = load_existing_dates(csv_path)
    if existing_dates:
        start = date.fromisoformat(max(existing_dates)) + timedelta(days=1)
    else:
        start = HISTORY_START_DATE
    if start > yesterday:
        return None
    return start, yesterday


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------


def get_api_key() -> str:
    """Return the weather.com API key.

    Checks the WU_API_KEY environment variable first; if not set, extracts
    the key from the Weather Underground dashboard page source.
    """
    api_key = os.environ.get("WU_API_KEY")
    if api_key:
        return api_key

    url = f"https://www.wunderground.com/dashboard/pws/{STATION_ID}"
    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30)
    resp.raise_for_status()

    for pattern in [
        r'"apiKey"\s*:\s*"([a-f0-9]{32})"',
        r'apiKey=([a-f0-9]{32})',
        r'"key"\s*:\s*"([a-f0-9]{32})"',
    ]:
        match = re.search(pattern, resp.text)
        if match:
            return match.group(1)

    raise RuntimeError(
        "API key not found in page source. "
        "Set the WU_API_KEY environment variable manually."
    )


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


_CHUNK_DAYS = 30  # API rejects ranges larger than ~31 days


def fetch_observations(station_id: str, start_date: date, end_date: date, api_key: str) -> list[dict]:
    """Fetch daily summary observations for a date range (inclusive).

    Splits large ranges into 30-day chunks to stay within API limits.
    Uses metric units (°C, km/h, hPa).  Returns a list of observation dicts
    as returned by the weather.com API.
    """
    url = "https://api.weather.com/v2/pws/history/daily"
    all_observations: list[dict] = []
    chunk_start = start_date

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=_CHUNK_DAYS - 1), end_date)
        params = {
            "stationId": station_id,
            "format": "json",
            "units": "m",  # metric: °C, km/h, hPa
            "startDate": chunk_start.strftime("%Y%m%d"),
            "endDate": chunk_end.strftime("%Y%m%d"),
            "apiKey": api_key,
            "numericPrecision": "decimal",
        }
        resp = requests.get(url, params=params, headers=_BROWSER_HEADERS, timeout=30)
        resp.raise_for_status()
        all_observations.extend(resp.json().get("observations", []))
        chunk_start = chunk_end + timedelta(days=1)

    return all_observations


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_observation(obs: dict) -> dict:
    """Convert a raw API observation dict into a CSV-row dict."""
    obs_date = (obs.get("obsTimeLocal") or obs.get("obsTimeUtc") or "")[:10]
    m = obs.get("metric", {})
    return {
        "Datum":                           obs_date,
        # Temperature
        "Temp_Hoch_°C":                    m.get("tempHigh", ""),
        "Temp_Avg_°C":                     m.get("tempAvg", ""),
        "Temp_Tief_°C":                    m.get("tempLow", ""),
        # Dew point
        "Taupunkt_Hoch_°C":                m.get("dewptHigh", ""),
        "Taupunkt_Avg_°C":                 m.get("dewptAvg", ""),
        "Taupunkt_Tief_°C":                m.get("dewptLow", ""),
        # Wind chill
        "Windchill_Hoch_°C":               m.get("windchillHigh", ""),
        "Windchill_Avg_°C":                m.get("windchillAvg", ""),
        "Windchill_Tief_°C":               m.get("windchillLow", ""),
        # Heat index
        "Hitzeindex_Hoch_°C":              m.get("heatindexHigh", ""),
        "Hitzeindex_Avg_°C":               m.get("heatindexAvg", ""),
        "Hitzeindex_Tief_°C":              m.get("heatindexLow", ""),
        # Humidity
        "Luftfeuchtigkeit_Hoch_%":         obs.get("humidityHigh", ""),
        "Luftfeuchtigkeit_Avg_%":          obs.get("humidityAvg", ""),
        "Luftfeuchtigkeit_Tief_%":         obs.get("humidityLow", ""),
        # Wind speed
        "Windgeschwindigkeit_Hoch_km/h":   m.get("windspeedHigh", ""),
        "Windgeschwindigkeit_Avg_km/h":    m.get("windspeedAvg", ""),
        "Windgeschwindigkeit_Tief_km/h":   m.get("windspeedLow", ""),
        # Wind gusts
        "Windböe_Hoch_km/h":               m.get("windgustHigh", ""),
        "Windböe_Avg_km/h":                m.get("windgustAvg", ""),
        "Windböe_Tief_km/h":               m.get("windgustLow", ""),
        # Wind direction
        "Windrichtung_Avg_°":              obs.get("winddirAvg", ""),
        # Pressure
        "Luftdruck_Max_hPa":               m.get("pressureMax", ""),
        "Luftdruck_Min_hPa":               m.get("pressureMin", ""),
        "Luftdruck_Trend_hPa":             m.get("pressureTrend", ""),
        # Solar / UV
        "Sonnenstrahlung_Hoch_W/m2":       obs.get("solarRadiationHigh", ""),
        "UV_Index_Hoch":                   obs.get("uvHigh", ""),
        # Precipitation
        "Niederschlag_Total_mm":           m.get("precipTotal", ""),
        "Niederschlagsrate_Max_mm/h":      m.get("precipRate", ""),
        # Quality control
        "QC_Status":                       obs.get("qcStatus", ""),
    }


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------


def load_existing_dates(csv_path: str) -> set[str]:
    """Return the set of date strings (YYYY-MM-DD) already in the CSV."""
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["Datum"] for row in reader}


def append_rows(csv_path: str, rows: list[dict]) -> None:
    """Append rows to the CSV, writing the header if the file is new."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run() -> None:
    result = get_fetch_date_range(CSV_PATH)
    if result is None:
        print("Daten sind aktuell – nichts zu tun.")
        return

    start, yesterday = result
    print(f"Hole Daten für {start.isoformat()} bis {yesterday.isoformat()} …")

    api_key = get_api_key()
    observations = fetch_observations(STATION_ID, start, yesterday, api_key)
    existing_dates = load_existing_dates(CSV_PATH)

    new_rows = []
    for obs in observations:
        row = parse_observation(obs)
        obs_date = row["Datum"]
        if not obs_date:
            continue
        if obs_date in existing_dates:
            continue
        new_rows.append(row)

    new_rows.sort(key=lambda r: r["Datum"])

    if new_rows:
        append_rows(CSV_PATH, new_rows)
        print(f"{len(new_rows)} Tage hinzugefügt: {[r['Datum'] for r in new_rows]}")
    else:
        print("Keine neuen Daten zum Hinzufügen.")


if __name__ == "__main__":
    run()
