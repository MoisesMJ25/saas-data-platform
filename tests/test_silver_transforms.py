"""
test_silver_transforms.py — Pruebas unitarias sobre las transformaciones Silver.

Cubre las funciones internas de silver.py que operan sobre DataFrames (sin
necesidad de Delta Lake), haciendo los tests rápidos y sin I/O.

Tests:
  1. test_unit_conversion_cs_to_st  — CS × 20 -> ST.
  2. test_tipo_entrega_filtering     — solo ZPRE/ZVE1/Z04/Z05 pasan; resto se descarta.
  3. test_field_anomaly_quarantine   — cantidad nula/neg/0 y precio nulo van a cuarentena.
  4. test_temporal_join_quarantine   — material sin match temporal en catálogo va a cuarentena.
  5. test_deduplicate_exact_rows     — filas idénticas en columnas de negocio se deduplicana.
"""
from __future__ import annotations

import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from saas_pipeline.silver import (
    _CS_TO_ST,
    _deduplicate,
    _normalize_and_flag,
    _quarantine_field_anomalies,
    _split_tipo_entrega,
    _temporal_join_and_quarantine,
)


# Fixture de SparkSession compartida (sin Delta — transformaciones puras)

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("test-silver-transforms")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# Schema mínimo de una fila Bronze procesada para Silver

_BASE_SCHEMA = StructType([
    StructField("fecha_proceso", StringType(),        nullable=True),
    StructField("transporte",    LongType(),          nullable=True),
    StructField("ruta",          LongType(),          nullable=True),
    StructField("tipo_entrega",  StringType(),        nullable=True),
    StructField("material",      StringType(),        nullable=True),
    StructField("precio",        DecimalType(28, 10), nullable=True),
    StructField("cantidad",      DecimalType(28, 10), nullable=True),
    StructField("unidad",        StringType(),        nullable=True),
    StructField("_tenant_id",    StringType(),        nullable=True),
    StructField("_batch_id",     StringType(),        nullable=True),
])

_DIM_SCHEMA = StructType([
    StructField("material",    StringType(),        nullable=True),
    StructField("descripcion", StringType(),        nullable=True),
    StructField("categoria",   StringType(),        nullable=True),
    StructField("precio_base", DecimalType(28, 10), nullable=True),
    StructField("valid_from",  DateType(),          nullable=True),
    StructField("valid_to",    DateType(),          nullable=True),
    StructField("is_current",  BooleanType(),       nullable=True),
])


def _row(**kwargs) -> Row:
    """Crea una fila base con defaults sensatos para columnas no especificadas."""
    defaults = {
        "fecha_proceso": "20250115",
        "transporte":    101,
        "ruta":          201,
        "tipo_entrega":  "ZPRE",
        "material":      "MAT001",
        "precio":        Decimal("10.0"),
        "cantidad":      Decimal("5.0"),
        "unidad":        "ST",
        "_tenant_id":    "sv",
        "_batch_id":     "batch-001",
    }
    defaults.update(kwargs)
    # DecimalType no acepta float: convertir automáticamente
    for field in ("precio", "cantidad"):
        v = defaults[field]
        if v is not None and isinstance(v, (int, float)):
            defaults[field] = Decimal(str(v))
    return Row(**defaults)


# Test 1: Conversión de unidades CS -> ST

class TestUnitConversion:
    def test_cs_multiplied_by_factor(self, spark):
        """Un registro CS=3 debe producir cantidad_normalizada_st=60 (3×20)."""
        df = spark.createDataFrame(
            [_row(cantidad=3.0, unidad="CS")],
            schema=_BASE_SCHEMA,
        )
        result = _normalize_and_flag(df)
        row = result.collect()[0]
        assert float(row["cantidad_normalizada_st"]) == pytest.approx(3.0 * _CS_TO_ST)

    def test_st_unchanged(self, spark):
        """Un registro ST=7 debe conservar cantidad_normalizada_st=7."""
        df = spark.createDataFrame(
            [_row(cantidad=7.0, unidad="ST")],
            schema=_BASE_SCHEMA,
        )
        result = _normalize_and_flag(df)
        row = result.collect()[0]
        assert float(row["cantidad_normalizada_st"]) == pytest.approx(7.0)

    def test_flags_routine(self, spark):
        """ZPRE y ZVE1 deben tener is_routine_delivery=True, is_bonus_delivery=False."""
        rows = [_row(tipo_entrega=t) for t in ("ZPRE", "ZVE1")]
        df     = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        result = _normalize_and_flag(df).collect()
        for row in result:
            assert row["is_routine_delivery"] is True
            assert row["is_bonus_delivery"]   is False

    def test_flags_bonus(self, spark):
        """Z04 y Z05 deben tener is_bonus_delivery=True, is_routine_delivery=False."""
        rows = [_row(tipo_entrega=t) for t in ("Z04", "Z05")]
        df     = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        result = _normalize_and_flag(df).collect()
        for row in result:
            assert row["is_bonus_delivery"]   is True
            assert row["is_routine_delivery"] is False



# Test 2: Filtrado de tipo_entrega

class TestTipoEntregaFiltering:
    def test_valid_tipos_pass(self, spark):
        """Los 4 tipos válidos deben pasar sin descarte."""
        rows = [_row(tipo_entrega=t) for t in ("ZPRE", "ZVE1", "Z04", "Z05")]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_valid, n_discarded = _split_tipo_entrega(df)
        assert df_valid.count() == 4
        assert n_discarded == 0

    def test_invalid_tipos_discarded(self, spark):
        """Tipos COBR y Z99 deben ser descartados (contabilizados, no persistidos)."""
        rows = [
            _row(tipo_entrega="ZPRE"),  # válido
            _row(tipo_entrega="COBR"),  # inválido
            _row(tipo_entrega="Z99"),   # inválido
        ]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_valid, n_discarded = _split_tipo_entrega(df)
        assert df_valid.count() == 1
        assert n_discarded == 2

    def test_mixed_batch(self, spark):
        """Un batch mixto produce el split correcto."""
        rows = [
            _row(tipo_entrega="ZVE1", ruta=1),
            _row(tipo_entrega="Z04",  ruta=2),
            _row(tipo_entrega="OTRO", ruta=3),
        ]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_valid, n_discarded = _split_tipo_entrega(df)
        assert df_valid.count() == 2
        assert n_discarded == 1


# Test 3: Cuarentena por anomalías de campo

class TestFieldAnomalyQuarantine:
    def test_null_cantidad_goes_to_quarantine(self, spark):
        """Registro con cantidad=None debe ir a cuarentena."""
        rows = [_row(cantidad=None), _row(cantidad=5.0)]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_clean, df_q = _quarantine_field_anomalies(df)
        assert df_clean.count() == 1
        assert df_q.count() == 1
        reason = df_q.collect()[0]["_quarantine_reason"]
        assert "cantidad" in reason

    def test_negative_cantidad_goes_to_quarantine(self, spark):
        """Registro con cantidad=-1 debe ir a cuarentena."""
        rows = [_row(cantidad=-1.0), _row(cantidad=2.0)]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_clean, df_q = _quarantine_field_anomalies(df)
        assert df_clean.count() == 1
        assert df_q.count() == 1

    def test_zero_cantidad_goes_to_quarantine(self, spark):
        """Registro con cantidad=0 debe ir a cuarentena."""
        rows = [_row(cantidad=0.0)]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_clean, df_q = _quarantine_field_anomalies(df)
        assert df_clean.count() == 0
        assert df_q.count() == 1

    def test_null_precio_goes_to_quarantine(self, spark):
        """Registro con precio=None debe ir a cuarentena."""
        rows = [_row(precio=None), _row(precio=10.0)]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_clean, df_q = _quarantine_field_anomalies(df)
        assert df_clean.count() == 1
        assert df_q.count() == 1
        assert df_q.collect()[0]["_quarantine_reason"] == "null_precio"

    def test_valid_row_passes(self, spark):
        """Fila con cantidad > 0 y precio > 0 no debe ir a cuarentena."""
        df = spark.createDataFrame([_row(cantidad=10.0, precio=5.0)], schema=_BASE_SCHEMA)
        df_clean, df_q = _quarantine_field_anomalies(df)
        assert df_clean.count() == 1
        assert df_q.count() == 0


# Test 4: Join temporal con dim_materials

class TestTemporalJoin:
    def _dim_row(self, material, valid_from, valid_to, is_current=True):
        return {
            "material":    material,
            "descripcion": f"Desc {material}",
            "categoria":   "CAT",
            "precio_base": Decimal("8.0"),
            "valid_from":  datetime.date.fromisoformat(valid_from),
            "valid_to":    datetime.date.fromisoformat(valid_to),
            "is_current":  is_current,
        }

    def test_material_in_range_enriched(self, spark):
        """Material con valid_from ≤ fecha_proceso ≤ valid_to debe enriquecerse."""
        df_fact = spark.createDataFrame(
            [_row(fecha_proceso="20250115", material="MAT001")],
            schema=_BASE_SCHEMA,
        )
        df_dim = spark.createDataFrame(
            [self._dim_row("MAT001", "2025-01-01", "2025-06-30")],
            schema=_DIM_SCHEMA,
        )
        df_enriched, df_q = _temporal_join_and_quarantine(df_fact, df_dim)
        assert df_enriched.count() == 1
        assert df_q.count() == 0
        assert df_enriched.collect()[0]["descripcion"] == "Desc MAT001"

    def test_material_not_in_catalog_quarantined(self, spark):
        """Material inexistente en catálogo debe ir a cuarentena."""
        df_fact = spark.createDataFrame(
            [_row(material="INEXISTENTE")],
            schema=_BASE_SCHEMA,
        )
        df_dim = spark.createDataFrame(
            [self._dim_row("MAT001", "2025-01-01", "2025-12-31")],
            schema=_DIM_SCHEMA,
        )
        df_enriched, df_q = _temporal_join_and_quarantine(df_fact, df_dim)
        assert df_enriched.count() == 0
        assert df_q.count() == 1
        assert df_q.collect()[0]["_quarantine_reason"] == "material_not_in_catalog"

    def test_material_outside_validity_quarantined(self, spark):
        """Material cuyo catálogo expiró antes de la fecha de la transacción -> cuarentena."""
        df_fact = spark.createDataFrame(
            # Transacción en mayo; catálogo solo cubre enero–marzo
            [_row(fecha_proceso="20250501", material="MAT001")],
            schema=_BASE_SCHEMA,
        )
        df_dim = spark.createDataFrame(
            [self._dim_row("MAT001", "2025-01-01", "2025-03-31", is_current=False)],
            schema=_DIM_SCHEMA,
        )
        df_enriched, df_q = _temporal_join_and_quarantine(df_fact, df_dim)
        assert df_enriched.count() == 0
        assert df_q.count() == 1

    def test_temporal_join_ignores_is_current_alone(self, spark):
        """
        Un material con is_current=False pero válido en rango debe enriquecer
        la transacción. El join usa valid_from/valid_to, no solo is_current.
        (Penaliza el uso exclusivo de is_current — sección 11 de la arquitectura.)
        """
        df_fact = spark.createDataFrame(
            [_row(fecha_proceso="20250115", material="MAT001")],
            schema=_BASE_SCHEMA,
        )
        df_dim = spark.createDataFrame(
            # is_current=False pero el intervalo sí cubre la fecha de la transacción
            [self._dim_row("MAT001", "2025-01-01", "2025-03-31", is_current=False)],
            schema=_DIM_SCHEMA,
        )
        df_enriched, df_q = _temporal_join_and_quarantine(df_fact, df_dim)
        assert df_enriched.count() == 1, (
            "El join temporal debe usar valid_from/valid_to, no is_current"
        )
        assert df_q.count() == 0



# Test 5: Deduplicación de filas exactas

class TestDeduplication:
    def test_exact_duplicates_removed(self, spark):
        """Dos filas idénticas en columnas de negocio -> una se descarta."""
        row = _row()
        df = spark.createDataFrame([row, row], schema=_BASE_SCHEMA)
        df_dedup, n_removed = _deduplicate(df)
        assert df_dedup.count() == 1
        assert n_removed == 1

    def test_distinct_rows_kept(self, spark):
        """Filas con diferente ruta -> ambas se conservan."""
        rows = [_row(ruta=1), _row(ruta=2)]
        df = spark.createDataFrame(rows, schema=_BASE_SCHEMA)
        df_dedup, n_removed = _deduplicate(df)
        assert df_dedup.count() == 2
        assert n_removed == 0

    def test_three_copies_become_one(self, spark):
        """Triple duplicado -> queda exactamente 1 fila, se reportan 2 removidos."""
        row = _row()
        df = spark.createDataFrame([row, row, row], schema=_BASE_SCHEMA)
        df_dedup, n_removed = _deduplicate(df)
        assert df_dedup.count() == 1
        assert n_removed == 2
