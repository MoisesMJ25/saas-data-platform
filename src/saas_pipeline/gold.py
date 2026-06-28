"""
gold.py — Capa Gold: daily_metrics_by_delivery_type por tenant.

Responsabilidades (sección 6.4 de la arquitectura):
  - Leer Silver fact_deliveries para el tenant y rango de fechas.
  - Agregar por (tenant_id, fecha_proceso, tipo_entrega).
  - Calcular métricas: total_units, total_revenue, active_routes, active_transports.
  - Escribir con idempotencia: overwrite por partición de fecha_proceso (recompute).

Nota de diseño: Gold es derivado, no autoritativo. Cada run sobreescribe las
particiones del rango procesado. total_revenue usa precio de la transacción
(columna 'precio' de Silver), no precio_base del catálogo (sección 6.4).
"""
from __future__ import annotations

import logging

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.config import gold_path, silver_path

logger = logging.getLogger(__name__)


# Helper interno

def _to_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


# Lógica de cálculo

def _compute_daily_metrics(df_fact: DataFrame, batch_id: str) -> DataFrame:
    """
    Calcula daily_metrics_by_delivery_type desde Silver fact_deliveries.

    Métricas por (tenant_id, fecha_proceso, tipo_entrega):
      - total_units:       suma de cantidad_normalizada_st (unidad: ST).
      - total_revenue:     suma de (cantidad_normalizada_st × precio_transaccion).
                           Precio de la transacción, NO precio_base del catálogo.
      - active_routes:     count distinct de ruta.
      - active_transports: count distinct de transporte.
    """
    return (
        df_fact
        .groupBy("tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_normalizada_st").alias("total_units"),
            F.sum(
                F.col("cantidad_normalizada_st") * F.col("precio")
            ).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_computed_at",  F.current_timestamp())
    )


def _write_gold_partition(
    df: DataFrame,
    tbl_path: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> None:
    """
    Escribe Gold con idempotencia: overwrite de la partición del rango procesado.

    replaceWhere garantiza que re-ejecutar el mismo rango sobreescriba exactamente
    esas particiones sin afectar fechas fuera del rango (sección 5.5).
    """
    replace_cond = (
        f"fecha_proceso >= '{start_yyyymmdd}' "
        f"AND fecha_proceso <= '{end_yyyymmdd}'"
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("fecha_proceso")
        .option("replaceWhere", replace_cond)
        .save(tbl_path)
    )


# Función pública

def process_daily_metrics(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
) -> int:
    """
    Construye daily_metrics_by_delivery_type para el tenant indicado.

    Lee Silver fact_deliveries, agrega y escribe Gold con idempotencia
    por partición de fecha_proceso.

    Args:
        spark:    SparkSession activa.
        cfg:      DictConfig fusionado para el tenant.
        tenant:   Código de tenant en minúscula.
        batch_id: UUID del batch (mismo que Silver para trazabilidad).

    Returns:
        Número de filas escritas en Gold.
    """
    start_yyyymmdd = _to_yyyymmdd(cfg.execution.start_date)
    end_yyyymmdd   = _to_yyyymmdd(cfg.execution.end_date)

    silver_tbl = silver_path(cfg, tenant, "fact_deliveries")
    gold_tbl   = gold_path(cfg, tenant, "daily_metrics_by_delivery_type")

    logger.info(
        "[gold] daily_metrics START | tenant=%s | batch=%s | rango=%s–%s",
        tenant, batch_id, start_yyyymmdd, end_yyyymmdd,
    )

    df_fact = (
        spark.read.format("delta").load(silver_tbl)
        .filter(
            (F.col("fecha_proceso") >= start_yyyymmdd)
            & (F.col("fecha_proceso") <= end_yyyymmdd)
        )
    )

    n_silver = df_fact.count()
    if n_silver == 0:
        logger.warning(
            "[gold] tenant=%s | 0 filas en Silver para el rango. Gold no se escribe.",
            tenant,
        )
        return 0

    df_gold = _compute_daily_metrics(df_fact, batch_id)
    n_gold  = df_gold.count()

    _write_gold_partition(df_gold, gold_tbl, start_yyyymmdd, end_yyyymmdd)

    logger.info(
        "[gold] daily_metrics DONE | tenant=%s | %d filas -> %s",
        tenant, n_gold, gold_tbl,
    )
    return n_gold
