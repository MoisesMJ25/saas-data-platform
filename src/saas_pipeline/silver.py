"""
silver.py — Capa Silver: dim_materials (SCD Type 2) y fact_deliveries.

Responsabilidades (sección 6.3 de la arquitectura):
  dim_materials:
    - Leer materials_catalog.csv (ya tiene estructura SCD Type 2).
    - MERGE INTO Silver con clave (material, valid_from).
    - Actualizar atributos si cambiaron; insertar si es versión nueva.

  fact_deliveries:
    1. Leer desde Bronze Delta para el tenant y rango de fechas.
    2. Deduplicar duplicados exactos (sección 5.6).
    3. Descartar tipo_entrega fuera de {ZPRE, ZVE1, Z04, Z05} (contabilizar).
    4. Cuarentena: cantidad nula/negativa/cero o precio nulo.
    5. Join temporal con dim_materials; cuarentena si sin match.
    6. Normalizar CS->ST (×20) sobre registros válidos.
    7. Agregar flags is_routine_delivery / is_bonus_delivery.
    8. MERGE INTO Silver con clave de negocio compuesta.
"""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from saas_pipeline.config import bronze_path, quarantine_path, raw_file_path, silver_path

logger = logging.getLogger(__name__)

# Reglas negocio

_VALID_TIPOS: frozenset[str] = frozenset({"ZPRE", "ZVE1", "Z04", "Z05"})
_ROUTINE_TIPOS: frozenset[str] = frozenset({"ZPRE", "ZVE1"})
_BONUS_TIPOS: frozenset[str] = frozenset({"Z04", "Z05"})
_CS_TO_ST: int = 20

# Clave de negocio para MERGE INTO fact_deliveries (sección 5.5)
_FACT_MERGE_KEY: list[str] = [
    "tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]

# Esquema del catálogo de materiales

_MATERIALS_SCHEMA = StructType(
    [
        StructField("material", StringType(), nullable=True),
        StructField("descripcion", StringType(), nullable=True),
        StructField("categoria", StringType(), nullable=True),
        StructField("precio_base", DecimalType(28, 10), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
    ]
)


# Helpers internos — conversión de fechas


def _to_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


# dim_materials — SCD Type 2


def _read_materials_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(_MATERIALS_SCHEMA).csv(path)


def _upsert_dim_materials(
    spark: SparkSession,
    source_df: DataFrame,
    table_path: str,
) -> None:
    """
    MERGE INTO dim_materials con lógica SCD Type 2.

    Clave: (material, valid_from) — identifica una versión.
    WHEN MATCHED y atributos cambiaron -> UPDATE.
    WHEN NOT MATCHED -> INSERT.

    El CSV fuente ya trae la estructura SCD Type 2 (valid_from / valid_to /
    is_current); el MERGE sincroniza la tabla Delta con esa verdad.
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        source_df.write.format("delta").mode("overwrite").save(table_path)
        logger.info("[silver/dim_materials] Primera escritura -> %s", table_path)
        return

    delta_tbl = DeltaTable.forPath(spark, table_path)
    (
        delta_tbl.alias("tgt")
        .merge(
            source_df.alias("src"),
            "tgt.material = src.material AND tgt.valid_from = src.valid_from",
        )
        .whenMatchedUpdate(
            condition=(
                "tgt.descripcion <> src.descripcion OR "
                "tgt.categoria   <> src.categoria   OR "
                "tgt.precio_base <> src.precio_base OR "
                "tgt.valid_to    <> src.valid_to    OR "
                "tgt.is_current  <> src.is_current"
            ),
            set={
                "descripcion": "src.descripcion",
                "categoria": "src.categoria",
                "precio_base": "src.precio_base",
                "valid_to": "src.valid_to",
                "is_current": "src.is_current",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("[silver/dim_materials] MERGE completado -> %s", table_path)


# Función pública — dim_materials


def process_dim_materials(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,  # noqa: ARG001  — futuro: añadir _batch_id a dim_materials
) -> int:
    """
    Carga el catálogo de materiales en Silver para el tenant indicado.

    Aunque el catálogo es compartido entre tenants, se escribe bajo la ruta
    del tenant siguiendo la convención de la arquitectura (sección 5.2).
    Ver docs/observations.md para la discusión sobre catálogo compartido vs. por tenant.

    Returns:
        Número de filas en el catálogo fuente.
    """
    raw_path = raw_file_path(cfg, cfg.sources.materials_file)
    tbl_path = silver_path(cfg, tenant, "dim_materials")
    source_df = _read_materials_csv(spark, raw_path)

    n = source_df.count()
    _upsert_dim_materials(spark, source_df, tbl_path)
    logger.info("[silver] dim_materials | tenant=%s | %d versiones en catálogo", tenant, n)
    return n


# Helpers internos — fact_deliveries


def _read_bronze_deliveries(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> DataFrame:
    """Lee la tabla Bronze Delta del tenant filtrando por rango de fechas."""
    b_path = bronze_path(cfg, tenant, "deliveries")
    return (
        spark.read.format("delta")
        .load(b_path)
        .filter(
            (F.col("fecha_proceso") >= start_yyyymmdd) & (F.col("fecha_proceso") <= end_yyyymmdd)
        )
    )


def _deduplicate(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Elimina duplicados exactos sobre columnas de negocio.

    Duplicados exactos no se persisten en cuarentena (sección 5.6): se
    contabilizan y se descarta la copia extra.

    Returns:
        (df_deduped, n_removed)
    """
    dedup_cols = [
        "fecha_proceso",
        "transporte",
        "ruta",
        "tipo_entrega",
        "material",
        "precio",
        "cantidad",
        "unidad",
        "_tenant_id",
    ]
    n_before = df.count()
    df_dedup = df.dropDuplicates(dedup_cols)
    return df_dedup, n_before - df_dedup.count()


def _split_tipo_entrega(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Separa registros con tipo_entrega válido/inválido.

    Inválidos: descarte (regla de negocio — no van a cuarentena, sección 5.6).

    Returns:
        (df_valid, n_discarded)
    """
    valid_list = list(_VALID_TIPOS)
    df_valid = df.filter(F.col("tipo_entrega").isin(valid_list))
    n_discarded = df.filter(~F.col("tipo_entrega").isin(valid_list)).count()
    return df_valid, n_discarded


def _quarantine_field_anomalies(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separa filas con anomalías de campo: cantidad nula/neg/cero o precio nulo.

    Returns:
        (df_clean, df_quarantine_with_reason)
    """
    bad_qty = F.col("cantidad").isNull() | (F.col("cantidad") <= 0)
    bad_precio = F.col("precio").isNull()
    is_bad = bad_qty | bad_precio

    df_quarantine = df.filter(is_bad).withColumn(
        "_quarantine_reason",
        F.when(bad_qty & bad_precio, "null_or_invalid_cantidad_and_precio")
        .when(bad_qty, "null_or_invalid_cantidad")
        .otherwise("null_precio"),
    )
    df_clean = df.filter(~is_bad)
    return df_clean, df_quarantine


def _temporal_join_and_quarantine(
    df: DataFrame,
    dim_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Join temporal entre fact (fecha_proceso YYYYMMDD) y dim_materials
    (valid_from / valid_to en DateType).

    Registros sin match en el catálogo para la fecha de la transacción
    van a cuarentena con reason='material_not_in_catalog'.

    Si el catálogo tiene intervalos solapados para un mismo material+fecha
    (dato sucio en origen), se conserva la versión con valid_from más reciente.

    Returns:
        (df_enriched, df_quarantine_catalog)
    """
    dim_side = dim_df.select(
        F.col("material").alias("_cat_material"),
        F.col("descripcion"),
        F.col("categoria"),
        F.col("precio_base"),
        F.col("valid_from").alias("cat_valid_from"),
        F.col("valid_to").alias("cat_valid_to"),
    )

    df_with_dt = df.withColumn(
        "_fecha_dt",
        F.make_date(
            F.substring(F.col("fecha_proceso"), 1, 4).cast("int"),
            F.substring(F.col("fecha_proceso"), 5, 2).cast("int"),
            F.substring(F.col("fecha_proceso"), 7, 2).cast("int"),
        ),
    )

    joined = df_with_dt.join(
        dim_side,
        (df_with_dt["material"] == dim_side["_cat_material"])
        & (df_with_dt["_fecha_dt"] >= dim_side["cat_valid_from"])
        & (df_with_dt["_fecha_dt"] <= dim_side["cat_valid_to"]),
        "left",
    ).drop("_cat_material", "_fecha_dt")

    # Deduplicar por solapamientos de intervalos: conservar valid_from más reciente
    w = Window.partitionBy(
        "fecha_proceso",
        "_tenant_id",
        "transporte",
        "ruta",
        "material",
        "tipo_entrega",
    ).orderBy(F.col("cat_valid_from").desc_nulls_last())

    joined = joined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Separar: sin match -> cuarentena
    df_quarantine = joined.filter(F.col("descripcion").isNull()).withColumn(
        "_quarantine_reason", F.lit("material_not_in_catalog")
    )
    df_enriched = joined.filter(F.col("descripcion").isNotNull())

    return df_enriched, df_quarantine


def _normalize_and_flag(df: DataFrame) -> DataFrame:
    """
    Convierte unidades CS->ST (×20) y añade flags de tipo de entrega.

    cantidad_normalizada_st: unidad común usada en Gold para total_units y revenue.
    """
    return (
        df.withColumn(
            "cantidad_normalizada_st",
            F.when(F.col("unidad") == "CS", F.col("cantidad") * _CS_TO_ST).otherwise(
                F.col("cantidad")
            ),
        )
        .withColumn(
            "is_routine_delivery",
            F.col("tipo_entrega").isin(list(_ROUTINE_TIPOS)),
        )
        .withColumn(
            "is_bonus_delivery",
            F.col("tipo_entrega").isin(list(_BONUS_TIPOS)),
        )
    )


def _add_silver_metadata(df: DataFrame, tenant: str, batch_id: str) -> DataFrame:
    """Añade tenant_id como columna de negocio y actualiza _batch_id con el batch Silver."""
    return (
        df.withColumn("tenant_id", F.lit(tenant))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )


def _write_silver_quarantine(
    spark: SparkSession,
    df: DataFrame,
    q_path: str,
    batch_id: str,
) -> None:
    """
    Escribe filas de cuarentena Silver con idempotencia por _batch_id.

    Usa replaceWhere si la tabla ya existe; overwrite directo en primera escritura.
    """
    if df.count() == 0:
        return

    if DeltaTable.isDeltaTable(spark, q_path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_batch_id = '{batch_id}'")
            .save(q_path)
        )
    else:
        df.write.format("delta").mode("overwrite").save(q_path)


def _merge_fact_deliveries(
    spark: SparkSession,
    df: DataFrame,
    tbl_path: str,
) -> None:
    """
    MERGE INTO Silver fact_deliveries con clave de negocio compuesta.

    WHEN MATCHED -> UPDATE ALL (idempotente: mismo dato produce el mismo resultado).
    WHEN NOT MATCHED -> INSERT ALL.
    """
    if not DeltaTable.isDeltaTable(spark, tbl_path):
        (df.write.format("delta").mode("overwrite").partitionBy("fecha_proceso").save(tbl_path))
        logger.info("[silver/fact_deliveries] Primera escritura -> %s", tbl_path)
        return

    merge_cond = " AND ".join(f"tgt.{col} = src.{col}" for col in _FACT_MERGE_KEY)
    delta_tbl = DeltaTable.forPath(spark, tbl_path)
    (
        delta_tbl.alias("tgt")
        .merge(df.alias("src"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("[silver/fact_deliveries] MERGE completado -> %s", tbl_path)


# Función pública — fact_deliveries


def process_fact_deliveries(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
) -> dict[str, int]:
    """
    Pipeline completo de Silver fact_deliveries para un tenant.

    Flujo:
      Bronze -> deduplica -> filtra tipos -> cuarentena campos ->
      join temporal -> cuarentena catálogo -> normaliza -> MERGE Silver.

    Args:
        spark:    SparkSession activa.
        cfg:      DictConfig fusionado para el tenant.
        tenant:   Código de tenant en minúscula.
        batch_id: UUID del batch Silver (común para dim + fact).

    Returns:
        {'written': N, 'quarantined': M, 'discarded': K, 'deduplicated': D}
    """
    start_yyyymmdd = _to_yyyymmdd(cfg.execution.start_date)
    end_yyyymmdd = _to_yyyymmdd(cfg.execution.end_date)

    logger.info(
        "[silver] fact_deliveries START | tenant=%s | batch=%s | rango=%s–%s",
        tenant,
        batch_id,
        start_yyyymmdd,
        end_yyyymmdd,
    )

    # 1. Leer Bronze
    df = _read_bronze_deliveries(spark, cfg, tenant, start_yyyymmdd, end_yyyymmdd)

    # 2. Deduplicar exactos
    df, n_deduped = _deduplicate(df)
    if n_deduped > 0:
        logger.info("[silver] tenant=%s | %d duplicados exactos eliminados", tenant, n_deduped)

    # 3. Filtrar tipo_entrega: inválidos -> descarte
    df, n_discarded = _split_tipo_entrega(df)
    logger.info(
        "[silver] tenant=%s | %d registros descartados (tipo_entrega inválido)",
        tenant,
        n_discarded,
    )

    # 4. Cuarentena: anomalías de campo (cantidad / precio)
    df, df_q_fields = _quarantine_field_anomalies(df)
    n_q_fields = df_q_fields.count()

    # 5. Cargar dim_materials Silver para el join temporal
    dim_path = silver_path(cfg, tenant, "dim_materials")
    dim_df = spark.read.format("delta").load(dim_path)

    # 6. Join temporal + cuarentena por material sin match
    df, df_q_catalog = _temporal_join_and_quarantine(df, dim_df)
    n_q_catalog = df_q_catalog.count()

    # 7. Normalizar unidades y añadir flags
    df = _normalize_and_flag(df)

    # 8. Añadir metadatos Silver (tenant_id, _batch_id, _silver_timestamp)
    df = _add_silver_metadata(df, tenant, batch_id)

    # Añadir metadatos Silver a cuarentenas también (para trazabilidad)
    df_q_fields = _add_silver_metadata(df_q_fields, tenant, batch_id)
    df_q_catalog = _add_silver_metadata(df_q_catalog, tenant, batch_id)

    # 9. MERGE INTO Silver fact_deliveries
    fact_path = silver_path(cfg, tenant, "fact_deliveries")
    n_written = df.count()

    if n_written > 0:
        _merge_fact_deliveries(spark, df, fact_path)
    else:
        logger.warning("[silver] tenant=%s | 0 registros válidos para Silver", tenant)

    # 10. Escribir cuarentena combinada (unionByName rellena nulos en columnas faltantes)
    n_quarantined = n_q_fields + n_q_catalog
    if n_quarantined > 0:
        df_quarantine = df_q_fields.unionByName(df_q_catalog, allowMissingColumns=True)
        q_path = quarantine_path(cfg, "silver", tenant, "fact_deliveries")
        _write_silver_quarantine(spark, df_quarantine, q_path, batch_id)

    logger.info(
        "[silver] fact_deliveries DONE | tenant=%s | written=%d | "
        "quarantined=%d (campos=%d, catálogo=%d) | discarded=%d | deduped=%d",
        tenant,
        n_written,
        n_quarantined,
        n_q_fields,
        n_q_catalog,
        n_discarded,
        n_deduped,
    )

    return {
        "written": n_written,
        "quarantined": n_quarantined,
        "discarded": n_discarded,
        "deduplicated": n_deduped,
    }
