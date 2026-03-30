"""Load ETL module"""
import sqlite3
import pandas as pd
from pathlib import Path
import config

def load_to_database(df):
    """Carga datos a SQLite database"""
    try:
        # Asegurar que el directorio existe
        config.ensure_directories()
        
        # Conexión a SQLite
        db_path = config.WAREHOUSE_DB
        conn = sqlite3.connect(str(db_path))
        
        # Guardar DataFrame en tabla SQLite
        table_name = "superstore"
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        
        # Verificación
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"✅ Datos cargados en SQLite: {row_count} filas en tabla '{table_name}'")
        
        # Guardar estadísticas
        stats = {
            "total_rows": row_count,
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "database": str(db_path)
        }
        
        conn.close()
        return stats
        
    except Exception as e:
        raise Exception(f"Error al cargar datos: {str(e)}")

def save_processed_data(df):
    """Guarda datos procesados en formato parquet"""
    try:
        config.ensure_directories()
        output_path = config.PROCESSED_FILE
        df.to_parquet(output_path, index=False)
        print(f"✅ Datos procesados guardados en: {output_path}")
        return str(output_path)
    except Exception as e:
        raise Exception(f"Error al guardar datos procesados: {str(e)}")

if __name__ == "__main__":
    print("load.py: poner aquí la carga al destino")


