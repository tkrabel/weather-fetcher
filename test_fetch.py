"""
test_fetch.py
-------------
Fetches one day of raw API data for PWS IGREIF68 and prints every field.
Run with:  python test_fetch.py
"""

import json
from datetime import date, timedelta

from src.weather_fetcher import STATION_ID, fetch_observations, get_api_key

yesterday = date.today() - timedelta(days=1)

print(f"Fetching data for {yesterday.isoformat()} …")
api_key = get_api_key()
observations = fetch_observations(STATION_ID, yesterday, yesterday, api_key)

if not observations:
    print("No observations returned.")
else:
    print(json.dumps(observations[0], indent=2))
