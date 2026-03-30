# Proyecto SuperStore Data Analytics ETL

Este proyecto contiene la estructura robusta para un pipeline ETL (Extract, Transform, Load) enfocado en el dataset de Superstore.

Carpetas principales:
- `data/original/`: dataset crudo antes de cualquier cambio (aquí va `Sample - Superstore.csv`)
- `data/staging/`: datos intermedios limpiados
- `data/processed/`: datos procesados listos para análisis
- `data/warehouse/`: destino final (SQLite en este ejemplo)
- `src/`: código ETL
- `tests/`: pruebas unitarias

## Setup inicial

1. Crear virtualenv:
   - `python -m venv .venv`
   - Windows: `.venv\Scripts\activate`, Linux/Mac: `source .venv/bin/activate`
2. Instalar dependencias:
   - `pip install -r requirements.txt`

## Ejecución del pipeline

1. Colocar el CSV original en `data/original/Sample - Superstore.csv`
2. `python src/extract.py`
3. `python src/transform.py`
4. `python src/load.py`

## Pruebas unitarias

`pytest -q tests/test_etl.py`

## Notas

- `src/config.py` define rutas y esquema esperado.
- `src/utils.py` maneja logging y validaciones.
- Cambia `src/load.py` para tu base de datos real si no quieres SQLite.

