from snowflake.snowpark.context import get_active_session
import pandas as pd
import os

# Obtener sesión Snowflake
session = get_active_session()

# Descargar archivo desde el stage
session.file.get(
    "@DATA_PRO.LOAD.MDF_DATA/DATOS/Encuesta.csv",
    "/tmp"
)

archivo = "/tmp/Encuesta.csv"

# Leer archivo
df = pd.read_csv(
    archivo,
    sep="|",
    encoding="utf-8-sig"
)

print("Columnas originales:")
print(df.columns.tolist())

print(f"Registros encontrados: {len(df)}")

# Renombrar columnas
df.columns = [
    "ID_ENCUESTA",
    "ACEPTA_PARTICIPAR",
    "AUTORIZA_DATOS",
    "TIPO_DOCUMENTO",
    "NUMERO_DOCUMENTO",
    "NOMBRES_APELLIDOS",
    "EDAD",
    "SEXO",
    "LOCALIDAD",
    "PESO_KG",
    "ANTECEDENTES"
]

# Ajustar tipos de datos

df["ID_ENCUESTA"] = (
    df["ID_ENCUESTA"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df["ID_ENCUESTA"] = pd.to_numeric(
    df["ID_ENCUESTA"],
    errors="coerce"
)

df["EDAD"] = pd.to_numeric(
    df["EDAD"],
    errors="coerce"
)

df["PESO_KG"] = (
    df["PESO_KG"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df["PESO_KG"] = pd.to_numeric(
    df["PESO_KG"],
    errors="coerce"
)

# Agregar auditoría
df["FECHA_CARGA"] = pd.Timestamp.now()
df["ARCHIVO_ORIGEN"] = "Encuesta.csv"

# Crear tabla destino
session.sql("""
CREATE OR REPLACE TABLE DATA_PRO.LOAD.ENCUESTAS_RAW (
    ID_ENCUESTA NUMBER,
    ACEPTA_PARTICIPAR VARCHAR,
    AUTORIZA_DATOS VARCHAR,
    TIPO_DOCUMENTO VARCHAR,
    NUMERO_DOCUMENTO VARCHAR,
    NOMBRES_APELLIDOS VARCHAR,
    EDAD NUMBER,
    SEXO VARCHAR,
    LOCALIDAD VARCHAR,
    PESO_KG NUMBER(10,2),
    ANTECEDENTES VARCHAR,
    FECHA_CARGA TIMESTAMP_NTZ,
    ARCHIVO_ORIGEN VARCHAR
)
""").collect()

# Convertir a Snowpark
df_sf = session.create_dataframe(df)

# Cargar tabla
df_sf.write.mode("overwrite").save_as_table(
    "DATA_PRO.LOAD.ENCUESTAS_RAW"
)

# Validación
print("Carga finalizada correctamente")

session.sql("""
SELECT COUNT(*) TOTAL_REGISTROS
FROM DATA_PRO.LOAD.ENCUESTAS_RAW
""").show()

session.sql("""
SELECT *
FROM DATA_PRO.LOAD.ENCUESTAS_RAW
LIMIT 10
""").show()