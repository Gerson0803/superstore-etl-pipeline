"""
Main ETL Pipeline Coordinator
Ejecuta el pipeline completo: Extract → Transform → Load
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Variables globales para el resumen
pipeline_summary = {
    "nombre_dataset": "Superstore",
    "inicio": None,
    "fin": None,
    "filas_extraidas": 0,
    "filas_transformadas": 0,
    "filas_cargadas": 0,
    "columnas": 0,
    "errores": [],
    "estado": "FALLIDO"
}

def save_summary_report():
    """Genera un resumen del pipeline en archivo txt"""
    report_path = Path(__file__).parent / "RESUMEN_PIPELINE.txt"
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("RESUMEN EJECUCIÓN PIPELINE ETL - SUPERSTORE\n")
        f.write("=" * 70 + "\n\n")
        
        f.write(f"Fecha/Hora inicio: {pipeline_summary['inicio']}\n")
        f.write(f"Fecha/Hora fin: {pipeline_summary['fin']}\n")
        f.write(f"Estado: {pipeline_summary['estado']}\n\n")
        
        f.write("-" * 70 + "\n")
        f.write("ESTADÍSTICAS DEL PIPELINE\n")
        f.write("-" * 70 + "\n")
        f.write(f"Dataset: {pipeline_summary['nombre_dataset']}\n")
        f.write(f"Filas extraídas: {pipeline_summary['filas_extraidas']:,}\n")
        f.write(f"Filas transformadas: {pipeline_summary['filas_transformadas']:,}\n")
        f.write(f"Filas cargadas: {pipeline_summary['filas_cargadas']:,}\n")
        f.write(f"Total de columnas: {pipeline_summary['columnas']}\n\n")
        
        if pipeline_summary['errores']:
            f.write("-" * 70 + "\n")
            f.write("ERRORES ENCONTRADOS\n")
            f.write("-" * 70 + "\n")
            for error in pipeline_summary['errores']:
                f.write(f"  ✗ {error}\n")
            f.write("\n")
        
        f.write("-" * 70 + "\n")
        f.write("ARCHIVOS GENERADOS\n")
        f.write("-" * 70 + "\n")
        f.write("  ✓ data/staging/superstore.csv\n")
        f.write("  ✓ data/processed/superstore_processed.parquet\n")
        f.write("  ✓ data/warehouse/superstore.db (tabla: superstore)\n")
        f.write("  ✓ etl_pipeline.log\n")
        f.write("  ✓ RESUMEN_PIPELINE.txt\n\n")
        
        f.write("=" * 70 + "\n")
    
    logger.info(f"✓ Resumen guardado en: {report_path}")


def run_pipeline():
    """Ejecuta el pipeline ETL completo"""
    pipeline_summary["inicio"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE ETL")
        logger.info("=" * 60)
        
        # STEP 1: EXTRACT
        logger.info("\n[1/3] EXTRAYENDO DATOS...")
        try:
            import extract
            read_data = extract.read_data
            file_path = "data/original/Sample - Superstore.csv" 
            data = read_data(file_path) 
            pipeline_summary["filas_extraidas"] = len(data)
            pipeline_summary["columnas"] = len(data.columns)
            logger.info("✓ Extracción completada")
        except Exception as e:
            error_msg = f"Error en extracción: {e}"
            logger.error(error_msg)
            pipeline_summary["errores"].append(error_msg)
            raise
        
        # STEP 2: TRANSFORM
        logger.info("\n[2/3] TRANSFORMANDO DATOS...")
        try:
            import transform
            data = transform.transform(data)
            pipeline_summary["filas_transformadas"] = len(data)
            logger.info("✓ Transformación completada")
        except Exception as e:
            error_msg = f"Error en transformación: {e}"
            logger.error(error_msg)
            pipeline_summary["errores"].append(error_msg)
            raise
        
        # STEP 3: LOAD
        logger.info("\n[3/3] CARGANDO DATOS...")
        try:
            import load
            
            # Guardar en parquet
            load.save_processed_data(data)
            
            # Cargar a SQLite
            stats = load.load_to_database(data)
            pipeline_summary["filas_cargadas"] = stats["total_rows"]
            
            logger.info("✓ Carga completada")
        except Exception as e:
            error_msg = f"Error en carga: {e}"
            logger.error(error_msg)
            pipeline_summary["errores"].append(error_msg)
            raise
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)
        
        pipeline_summary["estado"] = "EXITOSO"
        return True
        
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"✗ PIPELINE FALLIDO: {e}")
        logger.error("=" * 60)
        pipeline_summary["estado"] = "FALLIDO"
        return False
    
    finally:
        pipeline_summary["fin"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_summary_report()


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)

