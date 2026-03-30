"""Extract ETL module"""
import pandas as pd
import os
# TODO: implementar la lógica de extracción.
# 1. Leer archivo crudo original desde data/original/Sample - Superstore.csv
# 2. Validar columnas esperadas y filas mínimas
# 3. Normalizar nombres de columnas (strip, baja/alta)
# 4. Manejar errores (archivo no existe, codificación, filas inválidas)
# 5. Guardar el archivo de staging en data/staging/
def read_data(file_path):
    try: 
        if file_path.endswith('.csv'):
            try:
                # Intentar primero con UTF-8
                df = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                # Si falla, probar con latin1
                df = pd.read_csv(file_path, encoding="latin1")
        else:
            csv_path = file_path.replace('.xlsx', '.csv')
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path, encoding="utf-8")
                except UnicodeDecodeError:
                    df = pd.read_csv(csv_path, encoding="latin1")
            else:
                df = pd.read_excel(file_path)
        print(f"Archivo leido correctamente {file_path} #filas : {len(df)}, #columnas : {len(df.columns)} ")    
        df.to_csv("data/staging/superstore.csv", index=False)
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The file '{file_path}' was not found.")
    except PermissionError:
        raise PermissionError(f"Error: Permission denied when trying to read '{file_path}'.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file: {str(e)}")
    
    
if __name__ == "__main__":
    print("extract.py: poner aquí la extracción del dataset")



