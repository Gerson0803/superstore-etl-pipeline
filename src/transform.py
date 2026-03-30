"""Transform ETL module"""
import pandas as pd 


def transform(df):

    print(f"Dimensiones: {df.shape[0]} filas, {df.shape[1]} columnas\n")

    print("Tipos de datos:")
    print(df.dtypes, "\n")

    print("Valores nulos:")
    print(df.isnull().sum(), "\n")

    # 🔥 Limpieza básica
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 🔥 Eliminar duplicados
    df = df.drop_duplicates()
    
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
    # 🔥 Columnas derivadas
    print("Tipo Order Date:", df["Order Date"].dtype)
    print("Tipo Ship Date:", df["Ship Date"].dtype)
    df["Dias_Envio"] = (df["Ship Date"] - df["Order Date"]).dt.days
    df = df[df["Dias_Envio"] >= 0]


    # 🔥 Profit Margin seguro
    df["Profit_Margin"] = df["Profit"] / df["Sales"]
    df["Profit_Margin"] = df["Profit_Margin"].replace([float("inf"), -float("inf")], 0)
    df["Profit_Margin"] = df["Profit_Margin"].fillna(0)

    print("✅ Transformación completada\n")

    return df