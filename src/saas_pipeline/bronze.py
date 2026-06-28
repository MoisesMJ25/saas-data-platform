"""
bronze.py — Capa Bronze: ingesta de CSV raw a Delta con idempotencia por partición.

Responsabilidades (sección 6.2 de la arquitectura):
  - Leer el CSV con esquema explícito (sin inferencia costosa ni frágil).
  - Añadir columnas técnicas: _ingestion_timestamp, _source_file, _tenant_id, _batch_id.
  - Normalizar 'pais' a minúscula → _tenant_id (sección 5.3).
  - Filtrar por tenant y rango de fechas del run.
  - Escribir como Delta particionado por fecha_proceso con idempotencia vía replaceWhere.
  - Aislar en bronze_quarantine las filas con fecha_proceso nula, con formato inválido
    o con fecha de calendario imposible (p.ej. '20250230'), ya que no pueden asignarse
    a una partición (sección 5.6, primer tipo de anomalía).

Nota de diseño: las demás anomalías (cantidad <=0, material sin catálogo, precio nulo,
tipo_entrega inválido) son responsabilidad de la capa Silver (sección 6.3). Bronze
preserva el esquema original y solo descarta/aísla lo que impide la partición.
"""

from __future__ import annotations

import datetime
import logging
import uuid

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from saas_pipeline.config import bronze_path, quarantine_path, raw_file_path

logger = logging.getLogger(__name__)

# Esquema explícito del CSV de entregas (Evita inferencia y previene cambios silenciosos).

_DELIVERIES_CSV_SCHEMA = StructType(
    [
        StructField("pais", StringType(), nullable=True),
        StructField("fecha_proceso", StringType(), nullable=True),
        StructField("transporte", LongType(), nullable=True),
        StructField("ruta", LongType(), nullable=True),
        StructField("tipo_entrega", StringType(), nullable=True),
        StructField("material", StringType(), nullable=True),
        StructField("precio", DecimalType(28, 18), nullable=True),
        StructField("cantidad", DecimalType(28, 18), nullable=True),
        StructField("unidad", StringType(), nullable=True),
    ]
)

# Patrón YYYYMMDD: exactamente 8 dígitos numéricos
_YYYYMMDD_PATTERN = r"^\d{8}$"


@F.udf(BooleanType())
def _is_real_date(s: str) -> bool:
    """Python UDF para validar fecha calendario."""
    if not s:
        return False
    try:
        datetime.datetime.strptime(s, "%Y%m%d")
        return True
    except ValueError:
        return False


def _to_yyyymmdd(date_str: str) -> str:
    """Convierte 'YYYY-MM-DD' → 'YYYYMMDD' para comparar con fecha_proceso."""
    return date_str.replace("-", "")


def _read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Lee el CSV crudo de la fuente con esquema explícito."""
    return spark.read.option("header", "true").schema(_DELIVERIES_CSV_SCHEMA).csv(path)


def _add_technical_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Añade las 4 columnas técnicas requeridas (sección 6.2) y normaliza pais.

    _tenant_id es siempre minúscula; pais original se preserva sin cambios
    para trazabilidad y cumplimiento de 'esquema original preservado'.
    """
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_tenant_id", F.lower(F.col("pais")))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def _split_by_fecha_validity(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separa el DataFrame en filas con fecha_proceso válida e inválida.

    Válida: no nula, coincide con el patrón YYYYMMDD (8 dígitos) y representa
    una fecha de calendario real (descarta p.ej. '20250230').
    Inválida: nula, formato incorrecto o fecha imposible → va a bronze_quarantine.

    Returns:
        (df_valid, df_invalid)
    """
    has_value = F.col("fecha_proceso").isNotNull()
    has_format = F.col("fecha_proceso").rlike(_YYYYMMDD_PATTERN)
    is_real_date = _is_real_date(F.col("fecha_proceso"))

    valid_cond = has_value & has_format & is_real_date

    df_valid = df.filter(valid_cond)
    df_invalid = df.filter(~valid_cond).withColumn(
        "_quarantine_reason",
        F.when(~has_value, "null_fecha_proceso")
        .when(~has_format, "invalid_fecha_proceso_format")
        .otherwise("invalid_calendar_date"),
    )
    return df_valid, df_invalid


def _write_bronze_main(
    df: DataFrame,
    out_path: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> None:
    """
    Escribe la tabla Bronze principal con idempotencia via replaceWhere.

    replaceWhere sobreescribe SOLO las particiones de fecha_proceso
    comprendidas en el rango del run. Una rejecución con los mismos
    parámetros elimina y reinserta exactamente esas particiones.
    """
    replace_cond = f"fecha_proceso >= '{start_yyyymmdd}' AND fecha_proceso <= '{end_yyyymmdd}'"
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("fecha_proceso")
        .option("replaceWhere", replace_cond)
        .save(out_path)
    )


def _write_bronze_quarantine(df: DataFrame, out_path: str, batch_id: str) -> None:
    """
    Escribe filas con fecha_proceso inválida/nula en bronze_quarantine.

    Estrategia: overwrite condicionado al batch_id actual. Re-ejecutar el
    mismo batch sobreescribe sus propios registros de cuarentena; batches
    distintos se acumulan en la tabla (son auditables por _batch_id).
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_id = '{batch_id}'")
        .save(out_path)
    )


# Función pública principal


def ingest_deliveries(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str | None = None,
) -> dict[str, int]:
    """
    Ingesta el CSV de entregas para UN tenant en la capa Bronze.

    Args:
        spark:    SparkSession activa.
        cfg:      DictConfig fusionado (OmegaConf).
        tenant:   Código de tenant en minúscula ('sv', 'hn', …). Nunca 'all'.
        batch_id: UUID de lote; se genera uno si no se provee.

    Returns:
        Dict con conteos: {'written': N, 'quarantined': M}

    Raises:
        ValueError: Si tenant == 'all' (la iteración multi-tenant va en el CLI).
    """
    if tenant == "all":
        raise ValueError(
            "ingest_deliveries procesa un tenant a la vez. "
            "Para todos los tenants, itera desde el CLI."
        )

    batch_id = batch_id or str(uuid.uuid4())
    source_file = cfg.sources.deliveries_file
    raw_path = raw_file_path(cfg, source_file)
    start_yyyymmdd = _to_yyyymmdd(cfg.execution.start_date)
    end_yyyymmdd = _to_yyyymmdd(cfg.execution.end_date)

    logger.info(
        "[bronze] tenant=%s | batch=%s | rango=%s–%s | src=%s",
        tenant,
        batch_id,
        start_yyyymmdd,
        end_yyyymmdd,
        raw_path,
    )

    # 1. Leer CSV crudo con esquema explícito
    df_raw = _read_raw_csv(spark, raw_path)

    # 2. Añadir columnas técnicas y normalizar tenant
    df = _add_technical_columns(df_raw, source_file, batch_id)

    # 3. Filtrar al tenant de este run
    df = df.filter(F.col("_tenant_id") == tenant)

    # 4. Separar filas con fecha_proceso válida vs inválida/nula
    df_valid, df_invalid = _split_by_fecha_validity(df)

    # 5. Filtrar el rango de fechas sobre las filas válidas
    df_in_range = df_valid.filter(
        (F.col("fecha_proceso") >= start_yyyymmdd) & (F.col("fecha_proceso") <= end_yyyymmdd)
    )

    # 6. Contar antes de escribir (trigger de acción único por DataFrame)
    written = df_in_range.count()
    quarantined = df_invalid.count()

    # 7. Escribir tabla principal Bronze
    if written > 0:
        out_path = bronze_path(cfg, tenant, "deliveries")
        _write_bronze_main(df_in_range, out_path, start_yyyymmdd, end_yyyymmdd)
        logger.info(
            "[bronze] tenant=%s: %d filas → %s",
            tenant,
            written,
            out_path,
        )
    else:
        logger.warning(
            "[bronze] tenant=%s: 0 filas en rango %s–%s. No se escribe tabla principal.",
            tenant,
            start_yyyymmdd,
            end_yyyymmdd,
        )

    # 8. Escribir bronze_quarantine para fechas inválidas/nulas
    if quarantined > 0:
        q_path = quarantine_path(cfg, "bronze", tenant, "deliveries")
        _write_bronze_quarantine(df_invalid, q_path, batch_id)
        logger.warning(
            "[bronze] tenant=%s: %d filas con fecha_proceso inválida/nula → %s",
            tenant,
            quarantined,
            q_path,
        )

    return {"written": written, "quarantined": quarantined}
