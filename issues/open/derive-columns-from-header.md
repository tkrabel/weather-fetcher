# Can we derive date and precipitation column indices from the header row?

Instead of hardcoding `DATE_COLUMN = 2` and `PRECIP_COLUMN = 6`, can we read the header row and find the correct column indices dynamically based on the column names? This would make the script more robust if columns are ever reordered.
