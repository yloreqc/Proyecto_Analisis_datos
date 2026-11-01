from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime

# =====================================================
# SESION
# =====================================================

session = get_active_session()

# =====================================================
# CARGAR DATA DESDE LOAD
# =====================================================

df_load = session.table(
    "DATA_PRO.LOAD.ENCUESTAS_RAW"
).to_pandas()

if len(df_load) == 0:
    raise Exception(
        "La tabla DATA_PRO.LOAD.ENCUESTAS_RAW no contiene registros."
    )

print("=" * 80)
print("PERFILAMIENTO INICIAL")
print("=" * 80)

print(f"Total Registros LOAD: {len(df_load)}")

# =====================================================
# NULOS
# =====================================================

print("\nNULOS POR CAMPO")

nulos = pd.DataFrame({
    "CAMPO": df_load.columns,
    "NULOS": df_load.isnull().sum().values
})

nulos["PORCENTAJE"] = (
    nulos["NULOS"] / len(df_load) * 100
).round(2)

print(nulos)

# =====================================================
# DUPLICADOS
# =====================================================

duplicados = (
    df_load.groupby("NUMERO_DOCUMENTO")
    .size()
    .reset_index(name="CANTIDAD")
)

duplicados = duplicados[
    duplicados["CANTIDAD"] > 1
]

print("\nDOCUMENTOS DUPLICADOS")
print(f"Total encontrados: {len(duplicados)}")

# =====================================================
# DISTRIBUCIONES
# =====================================================

print("\nAUTORIZA_DATOS")
print(
    df_load["AUTORIZA_DATOS"]
    .fillna("VACIO")
    .value_counts()
)

print("\nSEXO")
print(
    df_load["SEXO"]
    .fillna("VACIO")
    .value_counts()
)

print("\nLOCALIDAD")
print(
    df_load["LOCALIDAD"]
    .fillna("VACIO")
    .value_counts()
)

# =====================================================
# EDAD
# =====================================================

df_load["EDAD"] = pd.to_numeric(
    df_load["EDAD"],
    errors="coerce"
)

print("\nESTADISTICAS EDAD")
print(df_load["EDAD"].describe())

edad_invalida = df_load[
    (df_load["EDAD"] < 0)
    |
    (df_load["EDAD"] > 120)
]

# =====================================================
# PESO
# =====================================================

df_load["PESO_KG"] = (
    df_load["PESO_KG"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df_load["PESO_KG"] = pd.to_numeric(
    df_load["PESO_KG"],
    errors="coerce"
)

print("\nESTADISTICAS PESO")
print(df_load["PESO_KG"].describe())

peso_invalido = df_load[
    (df_load["PESO_KG"] < 20)
    |
    (df_load["PESO_KG"] > 250)
]

# =====================================================
# ANTECEDENTES
# =====================================================

print("\nTOP ANTECEDENTES")

print(
    df_load["ANTECEDENTES"]
    .fillna("VACIO")
    .value_counts()
    .head(20)
)

# =====================================================
# DEPURACION PARA EDWH
# =====================================================

df_edwh = df_load.copy()

# -----------------------------------------
# SOLO AUTORIZADOS
# -----------------------------------------

df_edwh["AUTORIZA_DATOS"] = (
    df_edwh["AUTORIZA_DATOS"]
    .astype(str)
    .str.upper()
    .str.strip()
)

registros_originales = len(df_edwh)

df_edwh = df_edwh[
    df_edwh["AUTORIZA_DATOS"] == "SI"
]

registros_eliminados_habeas = (
    registros_originales - len(df_edwh)
)

# -----------------------------------------
# NORMALIZAR ANTECEDENTES
# -----------------------------------------

df_edwh["ANTECEDENTES"] = (
    df_edwh["ANTECEDENTES"]
    .astype(str)
    .str.strip()
)

# Convertir textos equivalentes a NULL

df_edwh.loc[
    df_edwh["ANTECEDENTES"].str.upper().isin([
        "NINGUNA DE ELLAS",
        "NO APLICA",
        "",
        "NAN",
        "NONE",
        "NULL"
    ]),
    "ANTECEDENTES"
] = None

# Excluir registros sin antecedentes

registros_sin_antecedentes = (
    df_edwh["ANTECEDENTES"].isnull().sum()
)

df_edwh = df_edwh[
    df_edwh["ANTECEDENTES"].notnull()
]

# -----------------------------------------
# ELIMINAR DUPLICADOS
# -----------------------------------------

antes_dup = len(df_edwh)

df_edwh = df_edwh.drop_duplicates(
    subset=["NUMERO_DOCUMENTO"]
)

duplicados_eliminados = (
    antes_dup - len(df_edwh)
)

# -----------------------------------------
# AUDITORIA
# -----------------------------------------

df_edwh["FECHA_PROCESAMIENTO"] = datetime.now()

# =====================================================
# RESUMEN CALIDAD
# =====================================================

print("\n")
print("=" * 80)
print("RESUMEN CALIDAD")
print("=" * 80)

print(f"Registros LOAD: {len(df_load)}")

print(
    f"Excluidos por Habeas Data: "
    f"{registros_eliminados_habeas}"
)

print(
    f"Excluidos por antecedentes vacíos/no aplica: "
    f"{registros_sin_antecedentes}"
)

print(
    f"Duplicados eliminados: "
    f"{duplicados_eliminados}"
)

print(
    f"Edades inválidas: "
    f"{len(edad_invalida)}"
)

print(
    f"Pesos inválidos: "
    f"{len(peso_invalido)}"
)

print(
    f"Total registros EDWH: "
    f"{len(df_edwh)}"
)

# =====================================================
# CREAR TABLA EDWH
# =====================================================

session.sql("""
CREATE OR REPLACE TABLE DATA_PRO.EDWH.ENCUESTAS (
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
    ARCHIVO_ORIGEN VARCHAR,
    FECHA_PROCESAMIENTO TIMESTAMP_NTZ
)
""").collect()

# =====================================================
# CARGAR EDWH
# =====================================================

session.create_dataframe(
    df_edwh
).write.mode("overwrite").save_as_table(
    "DATA_PRO.EDWH.ENCUESTAS"
)

print("\nCarga finalizada correctamente")

# =====================================================
# VALIDACIONES FINALES
# =====================================================

session.sql("""
SELECT
    COUNT(*) TOTAL_REGISTROS
FROM DATA_PRO.EDWH.ENCUESTAS
""").show()

session.sql("""
SELECT
    COUNT(*) REGISTROS_SIN_ANTECEDENTES
FROM DATA_PRO.EDWH.ENCUESTAS
WHERE ANTECEDENTES IS NULL
""").show()

session.sql("""
SELECT
    COUNT(*) REGISTROS_HABEAS_NO
FROM DATA_PRO.EDWH.ENCUESTAS
WHERE UPPER(AUTORIZA_DATOS) <> 'SI'
""").show()

session.sql("""
SELECT *
FROM DATA_PRO.EDWH.ENCUESTAS
LIMIT 10
""").show()