from snowflake.snowpark.context import get_active_session
import pandas as pd
import re
import unicodedata
from datetime import datetime

# =====================================================
# SESION
# =====================================================

session = get_active_session()

session.sql("USE DATABASE DATA_PRO").collect()

session.sql("""
CREATE SCHEMA IF NOT EXISTS DATA_GOLD
""").collect()

# =====================================================
# LEER ENCUESTAS
# =====================================================

df = session.table(
    "DATA_PRO.EDWH.ENCUESTAS"
).to_pandas()

print(f"Registros encontrados: {len(df)}")

# =====================================================
# FUNCION GENERICA
# =====================================================

def evaluar_habito(texto, positivos, negativos):

    texto_original = "" if texto is None else str(texto)
    # Normalizar tildes y espacios para que, por ejemplo, HÁBITO y HABITO
    # se evalúen igual y las frases partidas por saltos de línea coincidan.
    texto = unicodedata.normalize("NFD", texto_original.upper())
    texto = "".join(
        caracter for caracter in texto
        if unicodedata.category(caracter) != "Mn"
    )
    texto = re.sub(r"\s+", " ", texto).strip()

    for patron in negativos:

        match = re.search(patron, texto)

        if match:

            return (
                "NO",
                "Negación explícita encontrada",
                match.group(0),
                "ALTA"
            )

    for patron in positivos:

        match = re.search(patron, texto)

        if match:

            return (
                "SI",
                "Afirmación explícita encontrada",
                match.group(0),
                "ALTA"
            )

    return (
        "NO INFORMADO",
        "No existe evidencia suficiente",
        "",
        "BAJA"
    )

# =====================================================
# PATRONES LICOR
# =====================================================

LICOR_SI = [
    r'ALCOHOL',
    r'LICOR',
    r'CERVEZA',
    r'VINO',
    r'TOMO BEBIDAS',
    r'CONSUMO BEBIDAS',
    r'BEBIDAS ALCOH'
]

LICOR_NO = [
    r'NO CONSUMO ALCOHOL',
    r'NO TOMO LICOR',
    r'NO CONSUMO LICOR',
    r'\bNO\s+TENGO\s+(?:EL\s+)?HABITO\s+DE\s+TOMAR\s+LICOR\b',
    r'\bNO\s+TENGO\s+(?:EL\s+)?HABITO\s+DE\s+TOMAR\s+ALCOHOL\b',
    r'\bNO\s+TENGO\s+(?:EL\s+)?HABITO\s+DE\s+CONSUMIR\s+(?:LICOR|ALCOHOL)\b',
    r'\bNO\s+TOMO\s+(?:BEBIDAS\s+)?ALCOHOLICAS\b',
    r'\bNO\s+CONSUMO\s+BEBIDAS\s+ALCOHOLICAS\b',
    r'NO BEBO',
    r'ACTUALMENTE NO CONSUMO ALCOHOL'
]

# =====================================================
# PATRONES TABACO
# =====================================================

TABACO_SI = [
    r'\bSI\s+FUMO\b',
    r'\bFUMO\b',
    r'\bFUMADOR(?:A)?\b',
    r'\bFUMAR\s+CIGARRILLOS?\b',
    r'\b(?:CONSUMO|CONSUME)\s+(?:ACTUALMENTE\s+)?(?:TABACO|CIGARRILLOS?)\b',
    r'\bHABITO\s+DE\s+FUMAR\b',
    r'\bMANTENGO\s+EL\s+HABITO\s+DE\s+FUMAR(?:\s+TABACO)?\b'
]

TABACO_NO = [
    r'\bNO\s+FUMO\b',
    r'\bNO\s+SOY\s+FUMADOR(?:A)?\b',
    r'\bNO\s+CONSUMO\s+(?:TABACO|CIGARRILLOS?)\b',
    r'\bNIEGO\s+(?:EL\s+)?CONSUMO\s+DE\s+CIGARRILLOS?\b',
    r'\bNO\s+TENGO\s+(?:EL\s+)?HABITO\s+DE\s+FUMAR(?:\s+TABACO)?\b',
    r'\bNO\s+FUMADOR(?:A)?\b'
]

# =====================================================
# PATRONES VAPEADOR
# =====================================================

VAPE_SI = [
    r'VAPER',
    r'VAPEO',
    r'VAPEADOR',
    r'VAPEADORES',
    r'CIGARRILLO ELECTRONICO'
]

VAPE_NO = [
    r'NO USO VAPER',
    r'NO VAPEO',
    r'NO USO VAPEADORES',
    r'NO CONSUMO VAPER'
]

# =====================================================
# PATRONES ACTIVIDAD FISICA
# =====================================================

ACTIVIDAD_SI = [
    r'ACTIVIDAD FISICA',
    r'HAGO EJERCICIO',
    r'PRACTICO DEPORTE',
    r'GIMNASIO',
    r'CAMINO COMO EJERCICIO',
    r'ENTRENAMIENTO',
    r'CAMINATA'
]

ACTIVIDAD_NO = [
    r'NO HAGO EJERCICIO',
    r'NO REALIZO ACTIVIDAD FISICA',
    r'NO PRACTICO DEPORTE',
    r'NO REALIZO EJERCICIO',
    r'NO HAGO ACTIVIDAD FISICA'
]

# =====================================================
# PROCESAMIENTO
# =====================================================

resultado = []

for _, row in df.iterrows():

    antecedentes = str(row["ANTECEDENTES"])

    licor = evaluar_habito(
        antecedentes,
        LICOR_SI,
        LICOR_NO
    )

    tabaco = evaluar_habito(
        antecedentes,
        TABACO_SI,
        TABACO_NO
    )

    vape = evaluar_habito(
        antecedentes,
        VAPE_SI,
        VAPE_NO
    )

    actividad = evaluar_habito(
        antecedentes,
        ACTIVIDAD_SI,
        ACTIVIDAD_NO
    )

    resultado.append({

        "ID_ENCUESTA": row["ID_ENCUESTA"],
        "NUMERO_DOCUMENTO": str(row["NUMERO_DOCUMENTO"]),
        "NOMBRES_APELLIDOS": str(row["NOMBRES_APELLIDOS"]),
        "ANTECEDENTES": antecedentes,

        "CONSUME_LICOR": licor[0],
        "JUSTIFICA_CONSUME_LICOR": licor[1],
        "EVIDENCIA_LICOR": licor[2],
        "CONFIANZA_LICOR": licor[3],

        "FUMA_TABACO": tabaco[0],
        "JUSTIFICA_FUMA_TABACO": tabaco[1],
        "EVIDENCIA_TABACO": tabaco[2],
        "CONFIANZA_TABACO": tabaco[3],

        "USA_VAPEADOR": vape[0],
        "JUSTIFICA_USA_VAPEADOR": vape[1],
        "EVIDENCIA_VAPEADOR": vape[2],
        "CONFIANZA_VAPEADOR": vape[3],

        "REALIZA_ACTIVIDAD_FISICA": actividad[0],
        "JUSTIFICA_ACTIVIDAD_FISICA": actividad[1],
        "EVIDENCIA_ACTIVIDAD_FISICA": actividad[2],
        "CONFIANZA_ACTIVIDAD_FISICA": actividad[3],

        "FECHA_PROCESAMIENTO": datetime.now()
    })

# =====================================================
# DATAFRAME FINAL
# =====================================================

df_final = pd.DataFrame(resultado)

# Ajustar tipos

df_final["ID_ENCUESTA"] = pd.to_numeric(
    df_final["ID_ENCUESTA"],
    errors="coerce"
)

df_final["FECHA_PROCESAMIENTO"] = pd.to_datetime(
    df_final["FECHA_PROCESAMIENTO"]
)

print(df_final.dtypes)

# =====================================================
# ELIMINAR TABLA SI EXISTE
# =====================================================

session.sql("""
DROP TABLE IF EXISTS DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
""").collect()

# =====================================================
# CARGAR TABLA
# =====================================================

session.write_pandas(
    df=df_final,
    table_name="ENCUESTAS_HABITOS",
    database="DATA_PRO",
    schema="DATA_GOLD",
    auto_create_table=True,
    overwrite=True
)

print("Proceso terminado correctamente")

# =====================================================
# VALIDACIONES
# =====================================================

session.sql("""
SELECT COUNT(*) TOTAL_REGISTROS
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
""").show()

session.sql("""
SELECT
    FUMA_TABACO,
    COUNT(*) CANTIDAD
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
GROUP BY FUMA_TABACO
ORDER BY 2 DESC
""").show()

session.sql("""
SELECT
    CONSUME_LICOR,
    COUNT(*) CANTIDAD
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
GROUP BY CONSUME_LICOR
ORDER BY 2 DESC
""").show()

session.sql("""
SELECT
    USA_VAPEADOR,
    COUNT(*) CANTIDAD
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
GROUP BY USA_VAPEADOR
ORDER BY 2 DESC
""").show()

session.sql("""
SELECT
    REALIZA_ACTIVIDAD_FISICA,
    COUNT(*) CANTIDAD
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
GROUP BY REALIZA_ACTIVIDAD_FISICA
ORDER BY 2 DESC
""").show()

session.sql("""
SELECT *
FROM DATA_PRO.DATA_GOLD.ENCUESTAS_HABITOS
LIMIT 20
""").show()