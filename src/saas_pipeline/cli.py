"""
cli.py — Punto de entrada CLI del pipeline (saas-pipeline <comando> [OPCIONES]).

Comandos disponibles:
    bronze  Ejecuta solo la capa Bronze (ingesta CSV -> Delta).
    run     Ejecuta el pipeline completo: Bronze -> Silver -> Gold.

Opciones comunes:
    --env           Ambiente: dev | qa | main  (default: dev)
    --tenant        Código de tenant ('sv', 'hn', …) o 'all'  (default: all)
    --start-date    Inicio del rango fecha_proceso (YYYY-MM-DD)
    --end-date      Fin del rango fecha_proceso (YYYY-MM-DD)

Ejemplos:
    saas-pipeline bronze --env dev --tenant sv --start-date 2025-01-01 --end-date 2025-03-31
    saas-pipeline run    --env qa  --tenant all
"""

from __future__ import annotations

import logging
import sys
import uuid

import click
from omegaconf import OmegaConf

from saas_pipeline.bronze import ingest_deliveries
from saas_pipeline.config import list_configured_tenants, load_config, silver_path
from saas_pipeline.gold import process_daily_metrics
from saas_pipeline.quality import run_silver_checks, write_quality_log
from saas_pipeline.silver import process_dim_materials, process_fact_deliveries
from saas_pipeline.spark import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("saas_pipeline.cli")


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _apply_date_overrides(cfg, start_date: str | None, end_date: str | None):
    """Aplica overrides de fecha desde CLI sobre la config ya cargada."""
    overrides: dict = {}
    if start_date:
        overrides["start_date"] = start_date
    if end_date:
        overrides["end_date"] = end_date
    if overrides:
        return OmegaConf.merge(cfg, {"execution": overrides})
    return cfg


def _resolve_tenants(tenant: str) -> list[str]:
    """
    Devuelve la lista de tenants a procesar.

    Si tenant == 'all', lee los archivos en config/tenants/*.yaml.
    Falla rápido si no hay ninguno configurado.
    """
    if tenant != "all":
        return [tenant]
    tenants = list_configured_tenants()
    if not tenants:
        logger.error(
            "No se encontraron configs de tenant en config/tenants/. "
            "Crea al menos un archivo <código>.yaml."
        )
        sys.exit(1)
    return tenants


def _run_bronze_for_tenants(
    tenants: list[str],
    env: str,
    start_date: str | None,
    end_date: str | None,
    batch_id: str,
    base_cfg,
) -> list[tuple[str, str]]:
    """
    Ejecuta la ingesta Bronze para cada tenant.

    Gestiona el SparkSession de forma compartida (una sesión para todos los
    tenants del run) para evitar el overhead de crear/destruir JVM múltiples veces.

    Returns:
        Lista de (tenant, mensaje_error) para los tenants que fallaron.
    """
    spark = get_spark_session(base_cfg)
    errors: list[tuple[str, str]] = []

    for t in tenants:
        # Cargar config con overrides de tenant específico
        t_cfg = load_config(env=env, tenant=t)
        t_cfg = _apply_date_overrides(t_cfg, start_date, end_date)

        try:
            counts = ingest_deliveries(spark, t_cfg, tenant=t, batch_id=batch_id)
            logger.info(
                "[bronze] tenant=%s ✓ | written=%d | quarantined=%d",
                t,
                counts["written"],
                counts["quarantined"],
            )
        except Exception as exc:  # noqa: BLE001
            if base_cfg.execution.fail_fast:
                spark.stop()
                raise click.ClickException(
                    f"Fallo en tenant '{t}' (fail_fast=true): {exc}"
                ) from exc
            logger.error("[bronze] tenant=%s FALLÓ: %s", t, exc, exc_info=True)
            errors.append((t, str(exc)))

    spark.stop()
    return errors


# ---------------------------------------------------------------------------
# Decoradores de opciones comunes (evitan repetición en cada comando)
# ---------------------------------------------------------------------------


def _common_options(f):
    """Aplica las 4 opciones compartidas por todos los comandos del pipeline."""
    decorators = [
        click.option(
            "--env",
            default="dev",
            show_default=True,
            help="Ambiente de ejecución: dev | qa | main",
        ),
        click.option(
            "--tenant",
            default="all",
            show_default=True,
            help="Código de tenant en minúscula ('sv', 'hn', …) o 'all'",
        ),
        click.option(
            "--start-date",
            "start_date",
            default=None,
            metavar="YYYY-MM-DD",
            help="Inicio del rango de fecha_proceso (sobreescribe config)",
        ),
        click.option(
            "--end-date",
            "end_date",
            default=None,
            metavar="YYYY-MM-DD",
            help="Fin del rango de fecha_proceso (sobreescribe config)",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


# ---------------------------------------------------------------------------
# Grupo principal
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(package_name="saas-data-platform")
def main() -> None:
    """SAAS Data Platform — Pipeline Medallion Multi-Tenant."""


# ---------------------------------------------------------------------------
# Comando: bronze
# ---------------------------------------------------------------------------


@main.command()
@_common_options
def bronze(
    env: str,
    tenant: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Ingesta CSV raw -> Delta (capa Bronze) con idempotencia por partición."""
    batch_id = str(uuid.uuid4())
    tenants = _resolve_tenants(tenant)

    # Config base para Spark y para leer fail_fast (sin override de tenant)
    base_cfg = load_config(env=env, tenant="all")
    base_cfg = _apply_date_overrides(base_cfg, start_date, end_date)

    logger.info(
        "=== Bronze START | env=%s | tenants=%s | rango=%s–%s | batch=%s ===",
        env,
        tenants,
        base_cfg.execution.start_date,
        base_cfg.execution.end_date,
        batch_id,
    )

    errors = _run_bronze_for_tenants(tenants, env, start_date, end_date, batch_id, base_cfg)

    if errors:
        logger.error(
            "=== Bronze COMPLETADO CON ERRORES | tenants fallidos: %s ===",
            [t for t, _ in errors],
        )
        sys.exit(1)

    logger.info("=== Bronze COMPLETADO OK ===")


# ---------------------------------------------------------------------------
# Comando: run (pipeline completo)
# Silver y Gold se añadirán cuando sus módulos estén implementados.
# ---------------------------------------------------------------------------


@main.command()
@_common_options
def run(
    env: str,
    tenant: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """
    Ejecuta el pipeline completo: Bronze -> Silver -> Gold.

    Cada capa comparte la misma SparkSession y el mismo batch_id
    para trazabilidad end-to-end.
    """
    batch_id = str(uuid.uuid4())
    tenants = _resolve_tenants(tenant)
    base_cfg = load_config(env=env, tenant="all")
    base_cfg = _apply_date_overrides(base_cfg, start_date, end_date)

    logger.info(
        "=== Pipeline RUN START | env=%s | tenants=%s | batch=%s ===",
        env,
        tenants,
        batch_id,
    )

    # --- Bronze ---
    errors = _run_bronze_for_tenants(tenants, env, start_date, end_date, batch_id, base_cfg)
    if errors and base_cfg.execution.fail_fast:
        logger.error("Abortando pipeline por fail_fast=true.")
        sys.exit(1)

    # --- Silver ---
    spark = get_spark_session(base_cfg)
    quality_critical_tenants: list[str] = []

    for t in tenants:
        t_cfg = load_config(env=env, tenant=t)
        t_cfg = _apply_date_overrides(t_cfg, start_date, end_date)
        try:
            process_dim_materials(spark, t_cfg, tenant=t, batch_id=batch_id)
            silver_counts = process_fact_deliveries(spark, t_cfg, tenant=t, batch_id=batch_id)
            logger.info(
                "[silver] tenant=%s ✓ | written=%d | quarantined=%d | discarded=%d",
                t,
                silver_counts["written"],
                silver_counts["quarantined"],
                silver_counts["discarded"],
            )

            # Validaciones de calidad sobre Silver fact_deliveries ya escrito
            silver_df = spark.read.format("delta").load(silver_path(t_cfg, t, "fact_deliveries"))
            log_rows, has_critical = run_silver_checks(
                spark,
                silver_df,
                tenant=t,
                run_id=batch_id,
                batch_id=batch_id,
            )
            write_quality_log(spark, t_cfg, log_rows)

            if has_critical and t_cfg.quality.fail_on_critical:
                logger.error(
                    "[quality] tenant=%s: validación crítica falló. "
                    "Abortando antes de Gold (fail_on_critical=true).",
                    t,
                )
                quality_critical_tenants.append(t)

        except Exception as exc:  # noqa: BLE001
            if base_cfg.execution.fail_fast:
                spark.stop()
                raise click.ClickException(f"Fallo Silver en tenant '{t}': {exc}") from exc
            logger.error("[silver] tenant=%s FALLÓ: %s", t, exc, exc_info=True)
            errors.append((t, str(exc)))

    # --- Gold (solo tenants que pasaron calidad) ---
    for t in tenants:
        if t in quality_critical_tenants or any(e[0] == t for e in errors):
            logger.warning("[gold] tenant=%s omitido (falló Silver o calidad crítica).", t)
            continue
        t_cfg = load_config(env=env, tenant=t)
        t_cfg = _apply_date_overrides(t_cfg, start_date, end_date)
        try:
            n_gold = process_daily_metrics(spark, t_cfg, tenant=t, batch_id=batch_id)
            logger.info("[gold] tenant=%s ✓ | %d filas escritas", t, n_gold)
        except Exception as exc:  # noqa: BLE001
            if base_cfg.execution.fail_fast:
                spark.stop()
                raise click.ClickException(f"Fallo Gold en tenant '{t}': {exc}") from exc
            logger.error("[gold] tenant=%s FALLÓ: %s", t, exc, exc_info=True)
            errors.append((t, str(exc)))

    spark.stop()

    if errors:
        logger.error(
            "=== Pipeline COMPLETADO CON ERRORES | tenants fallidos: %s ===",
            [t for t, _ in errors],
        )
        sys.exit(1)

    logger.info("=== Pipeline RUN COMPLETADO OK ===")
