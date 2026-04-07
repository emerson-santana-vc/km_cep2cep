# IBGE City Codes Integration Guide

## Overview

This document describes how to use IBGE (Instituto Brasileiro de Geografia e Estatística) city codes to improve distance calculation accuracy in the km_cep2cep application.

## What is IBGE Code?

IBGE codes are unique identifiers for Brazilian municipalities (cities). Each city has a 7-digit code, e.g., `3550308` for São Paulo.

Example: São Paulo = 3550308, Rio de Janeiro = 3304557

## Features

### 1. **Database Storage**
- IBGE city reference table (`ibge_cities`) stores city information
- Distance requests and results now track origin/destination IBGE codes
- Enables audit trail and validation of geocoding results

### 2. **Data Extraction**
- When processing Oracle query results, IBGE codes are automatically extracted from:
  - `CIDADE_IBGE_ORIGEM` field
  - `CIDADE_IBGE_DESTINO` field
- Optional manual IBGE code input in single query and CSV upload modes

### 3. **City Reference Data**
- Use the provided `init_ibge_cities.py` script to populate the reference table
- Fetches data from the official IBGE public API
- Stores city name, state code, and coordinates (when available)

## Setup

### Initialize IBGE City Database

Run the initialization script to populate the IBGE city reference table from the official IBGE API:

```bash
python init_ibge_cities.py
```

**Note:** This requires internet connection to fetch data from `servicodados.ibge.gov.br`.

If the script completes successfully, you'll see:
```
Iniciando importação de cidades IBGE...
Conectando à API IBGE para buscar dados de cidades...
Encontradas 5570 cidades no Brasil.
Salvando dados no banco de dados...
✓ 5570 cidades importadas com sucesso!
```

## Usage

### In Streamlit UI - Oracle Search Tab

When using the **Pesquisa Oracle** (Oracle Search) tab:

1. Execute your Oracle query as normal
2. In the **"Processamento de Geolocalização e Rota"** section, the system automatically extracts:
   - `cidade_ibge_origem` → stored as `origin_ibge_code`
   - `cidade_ibge_destino` → stored as `destination_ibge_code`
3. These codes are saved with each distance result

**Requirement:** Your Oracle SQL query must include `CIDADE_IBGE_ORIGEM` and `CIDADE_IBGE_DESTINO` fields.

Example SQL:
```sql
SELECT tff.ENDERECO_ORIGEM,
       tff.ENDERECO_DESTINO,
       tff.CIDADE_ORIGEM,
       tff.CIDADE_IBGE_ORIGEM,
       tff.CIDADE_DESTINO,
       tff.CIDADE_IBGE_DESTINO,
       tff.UF
  FROM villa_origem_destino_notas_taff tff
 WHERE ...
```

### In Streamlit UI - Batch Upload Tab

When uploading CSV/XLSX files:

1. Select the **origin address** column
2. Select the **destination address** column
3. **Optional:** Select **IBGE code columns** for origin and destination
4. Process as usual

The IBGE codes (if provided) will be stored with the distance results for validation and auditing.

### In Streamlit UI - Single Query Tab

When performing single distance lookups:

1. Enter origin address
2. Enter destination address
3. **Optional:** Enter IBGE codes for origin and destination
4. Calculate distance

This helps validate and track individual lookups.

## Database Schema

### ibge_cities Table
```sql
CREATE TABLE ibge_cities (
    id INTEGER PRIMARY KEY,
    ibge_code VARCHAR(16) UNIQUE NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    state_code VARCHAR(2) NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Updated distance_requests Table
Added columns:
- `ibge_codigo_origem` VARCHAR(16) - Origin city IBGE code
- `ibge_codigo_destino` VARCHAR(16) - Destination city IBGE code

### Updated distance_results Table
Added columns:
- `origin_ibge_code` VARCHAR(16) - Origin city IBGE code
- `destination_ibge_code` VARCHAR(16) - Destination city IBGE code

## API Integration

### IBGE Public API

The system uses the official IBGE public API to fetch city data:

**Endpoint:** `https://servicodados.ibge.gov.br/api/v1`

**Available resources:**
- `/localidades/estados` - List all Brazilian states
- `/localidades/estados/{stateId}/municipios` - List municipalities in a state
- `/localidades/municipios/{cityCode}` - Get specific city info

**Example:**
```
https://servicodados.ibge.gov.br/api/v1/localidades/municipios/3550308
```

Response:
```json
{
  "id": 3550308,
  "nome": "São Paulo",
  "microrregiao": {
    "id": 3548,
    "nome": "Metropolitana de São Paulo",
    "mesorregiao": {
      "id": 3504,
      "nome": "Metropolitana de São Paulo",
      "estado": {
        "id": 35,
        "nome": "São Paulo",
        "sigla": "SP"
      }
    }
  }
}
```

## Usage Examples

### Example 1: Check if IBGE Code Exists

```python
from app.db.repository import get_city_from_ibge_code

city = get_city_from_ibge_code("3550308")  # São Paulo
if city:
    print(f"City: {city['city_name']}, State: {city['state_code']}")
else:
    print("City not found")
```

### Example 2: Add New City to Reference Table

```python
from app.db.repository import add_or_update_ibge_city

add_or_update_ibge_city(
    ibge_code="3550308",
    city_name="São Paulo",
    state_code="SP",
    latitude=-23.5505,
    longitude=-46.6333
)
```

### Example 3: Query Distance Results by IBGE Code

```python
from app.db.repository import SessionLocal
from app.db.repository import DistanceResult

session = SessionLocal()
results = session.query(DistanceResult).filter_by(origin_ibge_code="3550308").all()
session.close()

for result in results:
    print(f"Distance: {result.distance_km} km")
```

## Benefits

1. **Data Quality:** IBGE codes provide authoritative city identification
2. **Validation:** Can verify that geocoding results match the expected city code
3. **Audit Trail:** Track which city codes were used for each distance calculation
4. **Reusability:** Cache and reuse results for same origin/destination IBGE pairs
5. **Integration:** Enables future integration with other IBGE datasets (demographics, geographic data, etc.)

## Troubleshooting

### IBGE API Unreachable

If you see errors about IBGE API connectivity:

```
requests.exceptions.ConnectionError: Failed to connect to IBGE API
```

**Solution:** Check your internet connection and retry. The IBGE API is geographically distributed and usually very reliable.

### Missing IBGE Codes in Oracle Data

If your Oracle query returns NULL for IBGE code fields:

1. Verify the SQL query includes `CIDADE_IBGE_ORIGEM` and `CIDADE_IBGE_DESTINO`
2. Check that these fields are properly populated in your source table
3. The system will still work without IBGE codes, storing NULL values

### Duplicate IBGE Code Data

If you run `init_ibge_cities.py` multiple times:

- Existing entries will be updated (same IBGE code)
- No duplicate codes will be created due to UNIQUE constraint
- Safe to run multiple times

## Performance Notes

- IBGE city lookups are very fast (in-memory database queries)
- Initial population of 5,570+ cities takes ~2-5 minutes
- No ongoing performance impact on distance calculations

## Future Enhancements

Possible future improvements:

1. **Automatic Validation:** Validate geocoding results against IBGE city coordinates
2. **Smart CEP Lookup:** Use IBGE city to improve CEP validation accuracy
3. **Regional Analytics:** Group distances by IBGE regions and states
4. **Demographic Data:** Integrate population and geographic IBGE datasets
5. **Caching:** Cache IBGE lookups to improve repeated queries

## References

- [IBGE Servicodados API](https://servicodados.ibge.gov.br/)
- [IBGE Official Website](https://www.ibge.gov.br/)
- [Brazilian Municipality Codes](https://www.ibge.gov.br/explica/codigos-e-hierarquias-geograficas.php)
