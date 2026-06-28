"""
quality.py — Validaciones de calidad de datos y persistencia en quality_logs.

Responsabilidades (sección 5.9 y 6.5 de la arquitectura):
  - Definir el esquema de la tabla Delta quality_logs (cross-tenant).
  - Ejecutar ≥3 validaciones sobre Silver fact_deliveries.
  - Persistir resultados en append mode en data/shared/quality_logs/.
  - Exponer si alguna validación crítica falló para que el CLI pueda
    abortar antes de Gold cuando quality.fail_on_critical = true.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


# Esquema de quality_logs (sección 5.9)

QUALITY_LOG_SCHEMA = StructType([
    StructField("_run_id",         StringType(),   nullable=False),
    StructField("_batch_id",       StringType(),   nullable=False),
    StructField("tenant_id",       StringType(),   nullable=False),
    StructField("layer",           StringType(),   nullable=False),
    StructField("table_name",      StringType(),   nullable=False),
    StructField("check_name",      StringType(),   nullable=False),
    StructField("check_severity",  StringType(),   nullable=False),
    StructField("records_checked", LongType(),     nullable=False),
    StructField("records_failed",  LongType(),     nullable=False),
    StructField("check_passed",    BooleanType(),  nullable=False),
    StructField("executed_at",     TimestampType(), nullable=False),
])


# Helper interno

def _make_log_row(
    run_id: str,
    batch_id: str,
    tenant_id: str,
    layer: str,
    table_name: str,
    check_name: str,
    severity: str,
    records_checked: int,
    records_failed: int,
) -> dict:
    return {
        "_run_id":         run_id,
        "_batch_id":       batch_id,
        "tenant_id":       tenant_id,
        "layer":           layer,
        "table_name":      table_name,
        "check_name":      check_name,
        "check_severity":  severity,
        "records_checked": records_checked,
        "records_failed":  records_failed,
        "check_passed":    records_failed == 0,
        "executed_at":     datetime.now(UTC),
    }


# Escritura en quality_logs

def write_quality_log(
    spark: SparkSession,
    cfg: DictConfig,
    log_rows: list[dict],
) -> None:
    """
    Persiste filas de validación en la tabla Delta quality_logs (append).

    La tabla es cross-tenant; cada fila identifica el tenant por la columna
    tenant_id. Se usa append para acumular histórico de runs.
    """
    if not log_rows:
        return
    df = spark.createDataFrame(log_rows, schema=QUALITY_LOG_SCHEMA)
    (
        df.write
        .format("delta")
        .mode("append")
        .save(cfg.paths.quality_logs)
    )
    logger.info("[quality] %d registros escritos en quality_logs", len(log_rows))


# Validaciones sobre Silver fact_deliveries

def run_silver_checks(
    spark: SparkSession,
    df: DataFrame,
    tenant: str,
    run_id: str,
    batch_id: str,
) -> tuple[list[dict], bool]:
    """
    Ejecuta 4 validaciones sobre el DataFrame Silver fact_deliveries.

    Las validaciones se aplican sobre los registros ya procesados (listos
    para MERGE INTO Silver), no sobre la tabla Delta resultante, para evitar
    un read-back costoso.

    Checks:
      1. [critical] precio_no_nulo_ni_negativo   — precio debe ser > 0.
      2. [critical] cantidad_normalizada_positiva — cantidad_normalizada_st > 0.
      3. [warning]  clave_negocio_sin_duplicados  — unicidad de la clave de merge.
      4. [info]     enriquecimiento_con_catalogo  — descripcion no nula (join exitoso).

    Returns:
        (log_rows, has_critical_failure)
    """
    log_rows: list[dict] = []
    has_critical = False

    total = df.count()

    # ------------------------------------------------------
    # Check 1 [critical]: precio > 0
    # En Silver registros con precio nulo ya en cuarentena; este
    # check verifica la integridad de lo que llegó a Silver.

    failed_precio = df.filter(
        F.col("precio").isNull() | (F.col("precio") <= 0)
    ).count()

    log_rows.append(_make_log_row(
        run_id, batch_id, tenant, "silver", "fact_deliveries",
        "precio_no_nulo_ni_negativo", "critical", total, failed_precio,
    ))
    if failed_precio > 0:
        has_critical = True
        logger.error(
            "[quality] CRITICAL check 'precio_no_nulo_ni_negativo' FALLÓ: "
            "%d/%d filas con precio inválido | tenant=%s",
            failed_precio, total, tenant,
        )

    # ------------------------------------------------------
    # Check 2 [critical]: cantidad_normalizada_st > 0
    # Toda entrega válida debe tener cantidad positiva en unidad estándar.

    qty_col = "cantidad_normalizada_st"
    failed_qty = df.filter(
        F.col(qty_col).isNull() | (F.col(qty_col) <= 0)
    ).count()

    log_rows.append(_make_log_row(
        run_id, batch_id, tenant, "silver", "fact_deliveries",
        "cantidad_normalizada_positiva", "critical", total, failed_qty,
    ))
    if failed_qty > 0:
        has_critical = True
        logger.error(
            "[quality] CRITICAL check 'cantidad_normalizada_positiva' FALLÓ: "
            "%d/%d filas con cantidad_normalizada_st inválida | tenant=%s",
            failed_qty, total, tenant,
        )

    # ------------------------------------------------------
    # Check 3 [warning]: unicidad de clave de negocio
    # Detecta duplicados en (tenant_id, fecha_proceso, transporte, ruta,
    # material, tipo_entrega) antes del MERGE, lo que indicaría un problema
    # en la deduplicación upstream o en el CSV de origen.

    key_cols = ["tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    total_keys    = df.count()
    distinct_keys = df.select(*key_cols).distinct().count()
    failed_dups   = total_keys - distinct_keys

    log_rows.append(_make_log_row(
        run_id, batch_id, tenant, "silver", "fact_deliveries",
        "clave_negocio_sin_duplicados", "warning", total_keys, failed_dups,
    ))
    if failed_dups > 0:
        logger.warning(
            "[quality] WARNING check 'clave_negocio_sin_duplicados': "
            "%d claves duplicadas detectadas | tenant=%s",
            failed_dups, tenant,
        )

    # --------------------------------------------------------
    # Check 4 [info]: enriquecimiento con catálogo exitoso
    # Mide cuántos registros no pudieron ser enriquecidos con dim_materials.
    # Un valor > 0 indica materiales sin cobertura temporal en el catálogo
    # que no fueron capturados por cuarentena (anomalía de datos silenciosa).

    failed_enrich = df.filter(F.col("descripcion").isNull()).count()
    log_rows.append(_make_log_row(
        run_id, batch_id, tenant, "silver", "fact_deliveries",
        "enriquecimiento_completo_con_catalogo", "info", total, failed_enrich,
    ))
    if failed_enrich > 0:
        logger.info(
            "[quality] INFO check 'enriquecimiento_completo_con_catalogo': "
            "%d filas sin descripcion del catálogo | tenant=%s",
            failed_enrich, tenant,
        )

    return log_rows, has_critical
