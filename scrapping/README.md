# Loire-Bretagne RAG Data Scraper

Comprehensive scraping system for collecting water quality, hydrological, and geographic data for the Loire-Bretagne basin to build a RAG (Retrieval-Augmented Generation) dataset for territorial management queries.

## Features

- **Multi-source Data Collection**
  - Hub'Eau APIs (water quality, hydrometry, groundwater)
  - Geographic data (Admin Express, BD TOPAGE, ROE)
  - PDF documents (BSH bulletins, AELB quality reports)

- **Robust Infrastructure**
  - Resume capability (SQLite state management)
  - Per-domain rate limiting (respects playbook specifications)
  - Exponential backoff retry logic
  - Comprehensive JSONL logging
  - Data validation with Pydantic schemas

- **Processing Pipeline**
  - PDF OCR (text extraction with pytesseract)
  - Spatial operations (joins, snapping, buffer analysis)
  - Hydrological network graph construction

- **MCP Server**
  - Sitemap exploration tools
  - PDF link discovery
  - URL accessibility testing

## Architecture

```
scrapping/
├── scrapers/              # Data collection modules
│   ├── base.py           # Base classes with retry logic
│   ├── api_scrapers.py   # Hub'Eau API clients
│   ├── geo_downloaders.py # Geographic data downloaders
│   └── web_spiders.py    # Scrapy spiders for PDF crawling
├── processing/           # Data processing modules
│   ├── pdf_processor.py  # OCR and text extraction
│   ├── spatial_ops.py    # Spatial joins and operations
│   └── graph_builder.py  # Network graph construction
├── mcp_server/          # MCP server for sitemap exploration
│   ├── server.py        # MCP server implementation
│   └── tools.py         # MCP tools (explore_sitemap, find_pdf_links, etc.)
├── utils/               # Utility modules
│   ├── logger.py        # Structured logging (JSONL)
│   ├── rate_limiter.py  # Token bucket rate limiter
│   ├── state_manager.py # SQLite-based state management
│   └── validators.py    # Pydantic schemas
├── main.py              # Main orchestrator
├── requirements.txt     # Python dependencies
└── agent_scrape_playbook_loire_bretagne.json  # Task configuration
```

## Installation

### Prerequisites

- Python 3.9+
- Tesseract OCR (for PDF processing)
- GDAL/Fiona (for geospatial operations)

### System Dependencies (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-fra \
    poppler-utils \
    gdal-bin \
    libgdal-dev
```

### System Dependencies (macOS)

```bash
brew install tesseract tesseract-lang poppler gdal
```

### Python Dependencies

```bash
cd scrapping
pip install -r requirements.txt
```

## Usage

### Run All Tasks

Execute all tasks defined in the playbook (t1-t10 + post-processing):

```bash
cd scrapping
python main.py
```

### Run Specific Tasks

Run only specific tasks:

```bash
python main.py --tasks t1 t2 t3  # Only download geographic data
python main.py --tasks t4 t5 t6  # Only harvest API data
python main.py --tasks t7 t8     # Only crawl PDFs
```

### Resume After Interruption

The system automatically tracks completed operations. Simply re-run:

```bash
python main.py
# Already completed tasks will be skipped
```

### Force Re-run Completed Tasks

```bash
python main.py --no-skip-completed
```

### Adjust Logging Level

```bash
python main.py --log-level DEBUG
```

## Task Overview

### Tasks t1-t3: Geographic Data

- **t1**: Download Admin Express communes (GeoJSON)
- **t2**: Download BD TOPAGE hydrographic segments
- **t3**: Download ROE river obstacles

### Tasks t4-t6: API Harvesting

- **t4**: Hub'Eau Qualité Rivières (water quality analyses)
  - Parameters: NO3, TURB
  - Periods: 2015-2022, 2023
  - Output: `exports/parquet/qualite_rivieres.parquet`

- **t5**: Hub'Eau Hydrométrie (flow/discharge observations)
  - Variables: QmM (monthly mean), QmnJ (daily min)
  - Period: 2015-2025
  - Output: `exports/parquet/hydrometrie_obs_elab.parquet`

- **t6**: Hub'Eau Qualité Nappes (groundwater quality)
  - Parameters: NO3, TURB
  - Period: 2015-2025
  - Output: `exports/parquet/qualite_nappes.parquet`

### Tasks t7-t8: PDF Crawling

- **t7**: Crawl BSH (Bulletin de Situation Hydrologique) PDFs
  - Source: centre-val-de-loire.developpement-durable.gouv.fr
  - Output: `raw/pdfs/bsh/`

- **t8**: Crawl AELB quality documents
  - Source: donnees-documents.eau-loire-bretagne.fr
  - Output: `raw/pdfs/aelb/`

### Tasks t9-t10: PDF Processing

- **t9**: Extract text from AELB PDFs (OCR)
  - Input: `raw/pdfs/aelb/`
  - Output: `processed/text/aelb/`

- **t10**: Extract text from BSH PDFs (OCR)
  - Input: `raw/pdfs/bsh/`
  - Output: `processed/text/bsh/`

### Post-Processing (pp1-pp3)

- **pp1**: Spatial join - quality data with communes
  - Output: `exports/parquet/qualite_commune.parquet`

- **pp2**: Snap stations to hydrographic network
  - Buffer: 100m
  - Output: `exports/parquet/qualite_troncon.parquet`

- **pp3**: Build upstream/downstream graph
  - Output: `exports/json/topage_graph.json`

## MCP Server

Run the MCP server for sitemap exploration tools:

```bash
cd scrapping
python -m mcp_server.server
```

### Available MCP Tools

1. **explore_sitemap**: Explore XML sitemaps
   ```json
   {
     "url": "https://example.com/sitemap.xml"
   }
   ```

2. **find_pdf_links**: Find all PDF links on a page
   ```json
   {
     "url": "https://example.com/documents",
     "include_metadata": true
   }
   ```

3. **test_url**: Check URL accessibility
   ```json
   {
     "url": "https://example.com/document.pdf",
     "method": "HEAD"
   }
   ```

4. **find_all_links**: Find all links on a page
   ```json
   {
     "url": "https://example.com",
     "filter_domain": "example.com"
   }
   ```

## Data Outputs

### Directory Structure After Execution

```
raw/
  pdfs/
    bsh/              # BSH PDF bulletins
    aelb/             # AELB quality documents
processed/
  geo/
    admin_express_communes.geojson
    bd_topage_troncons.geojson
    roe_obstacles.geojson
  text/
    bsh/              # Extracted text from BSH
    aelb/             # Extracted text from AELB
exports/
  parquet/
    qualite_rivieres.parquet
    hydrometrie_obs_elab.parquet
    qualite_nappes.parquet
    qualite_commune.parquet
    qualite_troncon.parquet
  json/
    topage_graph.json
metadata/
  harvest_log.jsonl   # Structured logs
  state.db            # SQLite state database
```

## Configuration

Edit `agent_scrape_playbook_loire_bretagne.json` to customize:

- **Rate limits**: Adjust `max_rps` per domain
- **Retry settings**: Modify `retry.tries` and `backoff_seconds`
- **Concurrency**: Change `globals.concurrency`
- **Task parameters**: Update date ranges, parameter codes, etc.

## Rate Limiting

Respects playbook specifications:

- Hub'Eau APIs: 5 req/s
- AELB/BSH websites: 2 req/s
- data.gouv.fr: 2 req/s

Implemented using token bucket algorithm per domain.

## State Management & Resume

All operations are tracked in SQLite (`metadata/state.db`):

- Completed tasks
- Downloaded files (with hashes)
- API pages fetched
- PDF files processed

Re-running after interruption will skip completed operations.

## Logging

Logs are written to `metadata/harvest_log.jsonl` in structured JSON format:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "task_id": "t4",
  "source_id": "hubeau_qualite_rivieres_v2",
  "message": "Page fetched",
  "metrics": {
    "page": 5,
    "results": 1000,
    "total": 5000
  }
}
```

## Troubleshooting

### Tesseract Not Found

```bash
# Ubuntu/Debian
sudo apt-get install tesseract-ocr tesseract-ocr-fra

# macOS
brew install tesseract tesseract-lang

# Set TESSDATA_PREFIX if needed
export TESSDATA_PREFIX=/usr/share/tesseract-ocr/4.00/tessdata/
```

### GDAL/Fiona Issues

```bash
# Ubuntu/Debian
sudo apt-get install gdal-bin libgdal-dev
pip install gdal==$(gdal-config --version)

# macOS
brew install gdal
pip install gdal
```

### API Rate Limiting Errors

Adjust rate limits in playbook if receiving 429 errors:

```json
{
  "domain": "hubeau.eaufrance.fr",
  "max_rps": 2  // Reduce from 5 to 2
}
```

### PDF Processing Failures

Some PDFs may be scanned images requiring OCR:

```bash
# Ensure Tesseract French language data is installed
tesseract --list-langs | grep fra
```

## Development

### Run Tests

```bash
pytest tests/
```

### Check Data Quality

```python
import pandas as pd

# Load and inspect data
df = pd.read_parquet('exports/parquet/qualite_rivieres.parquet')
print(df.info())
print(df.describe())
print(df.isna().sum())
```

### Query Hydrological Graph

```python
from processing.graph_builder import GraphBuilder

builder = GraphBuilder('test')
graph = builder.load_graph('exports/json/topage_graph.json')

# Find upstream segments
upstream = builder.find_upstream_segments(graph, 'TRONC123456')
print(f"Found {len(upstream)} upstream segments")
```

## Contributing

1. Add new scrapers in `scrapers/`
2. Add new processing in `processing/`
3. Update playbook JSON with new tasks
4. Add task execution in `main.py`

## License

This project is for research and educational purposes related to territorial water management.

## Contact

For questions about data sources, refer to:

- Hub'Eau: https://hubeau.eaufrance.fr/
- AELB: https://www.eau-loire-bretagne.fr/
- IGN GeoServices: https://geoservices.ign.fr/

## Acknowledgments

Data sources:

- Hub'Eau (French water data API)
- Agence de l'Eau Loire-Bretagne
- IGN (French geographic institute)
- Sandre (French water data repository)
- DREAL Centre-Val de Loire
