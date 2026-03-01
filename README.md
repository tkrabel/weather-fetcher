# weather-fetcher

Fetches daily weather summaries for PWS station `IGREIF68` from the weather.com API and appends new days to `data/wetterdaten.csv`. Designed to run as a daily cron job — it self-heals by backfilling any missed days automatically.

## Run

```bash
uv run main.py
```

Optionally set your API key to skip auto-discovery:

```bash
WU_API_KEY=your_key uv run main.py
```
