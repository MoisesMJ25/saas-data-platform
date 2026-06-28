"""
test_quality.py — Pruebas unitarias sobre las validaciones de calidad (quality.py).

Verifica que run_silver_checks detecta correctamente fallos críticos y
que los log rows generados tienen la estructura correcta.
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from saas_pipeline.quality import QUALITY_LOG_SCHEMA, _make_log_row, run_silver_checks

# Fixture de SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("test-quality")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# Schema mínimo de Silver fact_deliveries para los quality checks

_SILVER_SCHEMA = StructType([
    StructField("tenant_id",              StringType(),        nullable=True),
    StructField("fecha_proceso",          StringType(),        nullable=True),
    StructField("transporte",             LongType(),          nullable=True),
    StructField("ruta",                   LongType(),          nullable=True),
    StructField("tipo_entrega",           StringType(),        nullable=True),
    StructField("material",               StringType(),        nullable=True),
    StructField("precio",                 DecimalType(28, 10), nullable=True),
    StructField("cantidad_normalizada_st", DecimalType(28, 10), nullable=True),
    StructField("descripcion",            StringType(),        nullable=True),
])


def _silver_row(**kwargs):
    from pyspark.sql import Row
    defaults = {
        "tenant_id":               "sv",
        "fecha_proceso":           "20250115",
        "transporte":              101,
        "ruta":                    201,
        "tipo_entrega":            "ZPRE",
        "material":                "MAT001",
        "precio":                  Decimal("10.0"),
        "cantidad_normalizada_st": Decimal("5.0"),
        "descripcion":             "Producto A",
    }
    defaults.update(kwargs)

    # DecimalType no acepta float: convertir automáticamente
    for field in ("precio", "cantidad_normalizada_st"):
        v = defaults[field]
        if v is not None and isinstance(v, (int, float)):
            defaults[field] = Decimal(str(v))
    return Row(**defaults)


# Tests sobre _make_log_row

class TestMakeLogRow:
    def test_check_passed_when_no_failures(self):
        """Si records_failed == 0, check_passed debe ser True."""
        row = _make_log_row(
            "run1", "batch1", "sv", "silver", "fact_deliveries",
            "mi_check", "critical", 100, 0,
        )
        assert row["check_passed"] is True
        assert row["records_checked"] == 100
        assert row["records_failed"] == 0

    def test_check_failed_when_there_are_failures(self):
        """Si records_failed > 0, check_passed debe ser False."""
        row = _make_log_row(
            "run1", "batch1", "sv", "silver", "fact_deliveries",
            "mi_check", "warning", 50, 5,
        )
        assert row["check_passed"] is False

    def test_all_required_fields_present(self):
        """El dict debe contener todas las claves del esquema quality_logs."""
        row = _make_log_row(
            "r", "b", "sv", "silver", "fact_deliveries", "c", "info", 10, 0,
        )
        schema_fields = {f.name for f in QUALITY_LOG_SCHEMA.fields}
        assert schema_fields.issubset(set(row.keys()))


# Tests sobre run_silver_checks

class TestRunSilverChecks:
    def test_clean_data_produces_no_critical(self, spark):
        """DataFrame Silver limpio no debe producir fallos críticos."""
        rows = [_silver_row() for _ in range(5)]
        df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        log_rows, has_critical = run_silver_checks(
            spark, df, tenant="sv", run_id="run1", batch_id="batch1",
        )
        assert has_critical is False
        # Todos los checks deben pasar
        critical_failed = [
            r for r in log_rows
            if r["check_severity"] == "critical" and not r["check_passed"]
        ]
        assert len(critical_failed) == 0

    def test_null_precio_triggers_critical(self, spark):
        """Silver con precio nulo debe disparar fallo crítico en check 1."""
        rows = [
            _silver_row(precio=None),  # precio nulo
            _silver_row(precio=10.0),
        ]
        df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        log_rows, has_critical = run_silver_checks(
            spark, df, tenant="sv", run_id="run1", batch_id="batch1",
        )
        assert has_critical is True
        precio_check = next(
            r for r in log_rows if r["check_name"] == "precio_no_nulo_ni_negativo"
        )
        assert not precio_check["check_passed"]
        assert precio_check["records_failed"] == 1

    def test_null_cantidad_normalizada_triggers_critical(self, spark):
        """Silver con cantidad_normalizada_st nula debe disparar fallo crítico en check 2."""
        rows = [_silver_row(cantidad_normalizada_st=None)]
        df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        log_rows, has_critical = run_silver_checks(
            spark, df, tenant="sv", run_id="run2", batch_id="batch2",
        )
        assert has_critical is True
        qty_check = next(
            r for r in log_rows if r["check_name"] == "cantidad_normalizada_positiva"
        )
        assert not qty_check["check_passed"]

    def test_log_rows_count_matches_checks(self, spark):
        """Deben generarse exactamente 4 log rows (4 validaciones implementadas)."""
        df = spark.createDataFrame([_silver_row()], schema=_SILVER_SCHEMA)
        log_rows, _ = run_silver_checks(
            spark, df, tenant="sv", run_id="run3", batch_id="batch3",
        )
        assert len(log_rows) == 4

    def test_log_rows_have_correct_tenant(self, spark):
        """Todos los log rows deben tener el tenant_id correcto."""
        df = spark.createDataFrame([_silver_row(tenant_id="hn")], schema=_SILVER_SCHEMA)
        log_rows, _ = run_silver_checks(
            spark, df, tenant="hn", run_id="run4", batch_id="batch4",
        )
        for row in log_rows:
            assert row["tenant_id"] == "hn"

    def test_duplicate_key_warning_check(self, spark):
        """Dos filas con la misma clave de negocio deben disparar el check de warning."""
        common = {"fecha_proceso": "20250115", "transporte": 1, "ruta": 1, "material": "M1", "tipo_entrega": "ZPRE"}
        rows = [_silver_row(**common), _silver_row(**common)]
        df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        log_rows, has_critical = run_silver_checks(
            spark, df, tenant="sv", run_id="run5", batch_id="batch5",
        )
        dup_check = next(
            r for r in log_rows if r["check_name"] == "clave_negocio_sin_duplicados"
        )
        assert not dup_check["check_passed"]
        assert dup_check["records_failed"] == 1
        # Los duplicados de clave son warning, no critical
        assert has_critical is False
