# Task t6 Scraping Modes

Task t6 supports two different scraping modes for groundwater quality data. The mode is **automatically selected** based on the playbook configuration - no code changes needed!

## Mode 1: Daily Iteration (Default)

**When to use:** Full dataset scraping for all regions

**Configuration:** No `nom_region` parameter in playbook

**How it works:**
- Queries the API day-by-day to avoid pagination limits
- Slower but comprehensive
- Files named: `qualite_nappes_NO3_2015.parquet`

**Playbook example:**
```json
{
  "id": "t6",
  "source": "hubeau_qualite_nappes_v1",
  "action": "api_harvest",
  "params": {
    "code_parametre": ["NO3", "TURB"],
    "periods": ["2015-01-01/2025-12-31"]
  },
  "output": "exports/parquet/qualite_nappes.parquet"
}
```

## Mode 2: Region Filter (Fast)

**When to use:** Scraping specific regions (e.g., Bretagne only)

**Configuration:** Add `nom_region` parameter in playbook

**How it works:**
- Uses the API's region filter for faster queries
- Makes yearly queries instead of daily
- Files named: `qualite_nappes_NO3_2015_Bretagne.parquet`

**Playbook example:**
```json
{
  "id": "t6",
  "source": "hubeau_qualite_nappes_v1",
  "action": "api_harvest",
  "params": {
    "code_parametre": ["NO3", "TURB"],
    "periods": ["2015-01-01/2025-12-31"],
    "nom_region": "Bretagne"
  },
  "output": "exports/parquet/qualite_nappes.parquet"
}
```

## How to Switch Modes

### Option 1: Edit the main playbook

Edit `agent_scrape_playbook_loire_bretagne.json`:

```bash
# For daily iteration (default)
# Remove or comment out the nom_region line

# For region filter
# Add "nom_region": "Bretagne" to params
```

### Option 2: Use prepared playbook files

We have pre-configured playbook files:

```bash
# Use daily iteration
cp agent_scrape_playbook_loire_bretagne.json playbook_active.json

# Use region filter
cp agent_scrape_playbook_loire_bretagne_with_region.json agent_scrape_playbook_loire_bretagne.json
```

Then run:
```bash
python main.py --tasks t6
```

## Available Region Values

Based on Hub'Eau API, valid region names include:
- "Bretagne"
- "Pays de la Loire"
- "Centre-Val de Loire"
- "Nouvelle-Aquitaine"
- (and other French regions)

## Performance Comparison

| Mode | Speed | API Calls | Use Case |
|------|-------|-----------|----------|
| Daily Iteration | Slow (~3-4 hours) | ~4,000 calls/year | Full national dataset |
| Region Filter | Fast (~30 min) | ~11 calls/year | Regional data only |

## Troubleshooting

**Log messages show which mode is active:**

```
# Daily iteration mode:
"Using daily iteration mode (no region filter)"

# Region filter mode:
"Using region filter mode", region="Bretagne"
```

**Check output filenames:**
- Daily mode: `qualite_nappes_NO3_2015.parquet`
- Region mode: `qualite_nappes_NO3_2015_Bretagne.parquet`
