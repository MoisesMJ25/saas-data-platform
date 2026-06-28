"""
Refactoring de bad_code.py — versión producción-ready.

Diferencias clave respecto al original:
  - Spark nativo (sin pandas, sin iterrows).
  - Configuración inyectada; cero paths o parámetros hardcoded.
  - Multi-tenant: el tenant se pasa como parámetro, no como filtro WHERE ad-hoc.
  - Anomaly handling explícito: cantidad <= 0 y precio nulo van a cuarentena.
  - Escritura Delta con idempotencia (replaceWhere por partición de fecha).
  - SparkSession inyectada -> función 100% testeable.
  - Tipado completo + logging.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

logger = logging.getLogger(__name__)


# Constantes de negocio (no hardcoded dentro de la función)

_ROUTINE_TIPOS = frozenset({"ZPRE", "ZVE1"})
_CS_TO_ST = 20
_DELIVERIES_SCHEMA = StructType([
    StructField("pais",          StringType(),        nullable=True),
    StructField("fecha_proceso", StringType(),        nullable=True),
    StructField("transporte",    LongType(),          nullable=True),
    StructField("ruta",          LongType(),          nullable=True),
    StructField("tipo_entrega",  StringType(),        nullable=True),
    StructField("material",      StringType(),        nullable=True),
    StructField("precio",        DecimalType(28, 10), nullable=True),
    StructField("cantidad",      DecimalType(28, 10), nullable=True),
    StructField("unidad",        StringType(),        nullable=True),
])


@dataclass
class ProcessResult:
    written:     int
    quarantined: int


def process_deliveries(
    spark: SparkSession,
    file_path: str,
    tenant: str,
    output_path: str,
    quarantine_path: str,
    fecha_proceso: str,
) -> ProcessResult:
    """
    Procesa entregas de un tenant para una fecha dada.

    Lee el CSV, filtra al tenant, aplica reglas de negocio sobre tipo_entrega y
    unidades, maneja anomalías con cuarentena explícita, y escribe en Delta con
    idempotencia por partición de fecha.

    Args:
        spark:           SparkSession activa (inyectada, no global).
        file_path:       Ruta al CSV de entregas.
        tenant:          Código de tenant en minúscula ('sv', 'gt', …).
        output_path:     Ruta Delta de salida (sin partición — se compone internamente).
        quarantine_path: Ruta Delta para registros con anomalías.
        fecha_proceso:   Fecha del proceso en formato YYYYMMDD.

    Returns:
        ProcessResult con conteos de filas escritas y en cuarentena.
    """
    # 1. Leer con esquema explícito (no inferencia — costosa y frágil)
    df = (
        spark.read
        .option("header", "true")
        .schema(_DELIVERIES_SCHEMA)
        .csv(file_path)
        .filter(F.lower(F.col("pais")) == tenant)
        .filter(F.col("fecha_proceso") == fecha_proceso)
    )

    # 2. Filtrar solo tipos de entrega de rutina (ZPRE, ZVE1)
    df = df.filter(F.col("tipo_entrega").isin(list(_ROUTINE_TIPOS)))

    # 3. Separar anomalías: cantidad inválida o precio nulo -> cuarentena
    bad_cond = (
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
        | F.col("precio").isNull()
    )
    df_quarantine = (
        df.filter(bad_cond)
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "invalid_cantidad")
            .otherwise("null_precio"),
        )
    )
    df_clean = df.filter(~bad_cond)

    # 4. Convertir unidades y calcular total (Spark vectorizado, sin loops Python)
    df_result = (
        df_clean
        .withColumn(
            "cantidad_st",
            F.when(F.col("unidad") == "CS", F.col("cantidad") * _CS_TO_ST)
            .otherwise(F.col("cantidad")),
        )
        .withColumn("total", F.col("cantidad_st") * F.col("precio"))
        .select(
            F.lower(F.col("pais")).alias("tenant_id"),
            "fecha_proceso",
            "material",
            "cantidad_st",
            "total",
        )
    )

    # 5. Escribir Delta con idempotencia: replaceWhere sobreescribe solo esta fecha
    n_written = df_result.count()
    if n_written > 0:
        (
            df_result.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("fecha_proceso")
            .option("replaceWhere", f"fecha_proceso = '{fecha_proceso}'")
            .save(output_path)
        )
    else:
        logger.warning(
            "tenant=%s | fecha=%s | 0 filas válidas. No se escribe output.",
            tenant, fecha_proceso,
        )

    # 6. Escribir cuarentena (append — acumula histórico de anomalías)
    n_quarantined = df_quarantine.count()
    if n_quarantined > 0:
        df_quarantine.write.format("delta").mode("append").save(quarantine_path)
        logger.warning(
            "tenant=%s | fecha=%s | %d filas en cuarentena",
            tenant, fecha_proceso, n_quarantined,
        )

    logger.info(
        "tenant=%s | fecha=%s | written=%d | quarantined=%d",
        tenant, fecha_proceso, n_written, n_quarantined,
    )
    return ProcessResult(written=n_written, quarantined=n_quarantined)
