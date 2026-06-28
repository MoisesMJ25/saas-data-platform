"""
config.py — Carga jerárquica de configuración con OmegaConf.

Jerarquía de merge (sección 5.8 de la prueba):
    base.yaml  ->  env/<env>.yaml  ->  tenants/<tenant>.yaml

Las claves de nivel superior se documentan aquí como dataclasses estructurados,
lo que da validación de tipos + autocompletado en IDE sin depender de Hydra.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)

# Raíz del proyecto: src/saas_pipeline/
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_CONFIG_DIR = _PROJECT_ROOT / "config"


# Structured config — sirve como schema y valores por defecto


@dataclass
class PathsConfig:
    raw: str = "data/raw"
    bronze: str = "data/bronze"
    silver: str = "data/silver"
    gold: str = "data/gold"
    quarantine_root: str = "data"
    quality_logs: str = "data/shared/quality_logs"


@dataclass
class ExecutionConfig:
    start_date: str = "2025-01-01"
    end_date: str = "2025-06-30"
    tenant: str = "all"
    fail_fast: bool = False


@dataclass
class QualityConfig:
    fail_on_critical: bool = False


@dataclass
class SparkConfig:
    app_name: str = "saas-pipeline"
    master: str = "local[*]"
    log_level: str = "WARN"
    shuffle_partitions: int = 8


@dataclass
class SourcesConfig:
    deliveries_file: str = "global_mobility_data_entrega_productos.csv"
    materials_file: str = "materials_catalog.csv"


@dataclass
class TenantMetaConfig:
    """Metadata opcional del tenant (campos informativos, no operacionales)."""

    code: str = ""
    display_name: str = ""
    country_code: str = ""


@dataclass
class PipelineConfig:
    paths: PathsConfig = field(default_factory=PathsConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    sources: SourcesConfig = field(default_factory=SourcesConfig)
    tenant: TenantMetaConfig = field(default_factory=TenantMetaConfig)


# Funciones públicas


def load_config(env: str, tenant: str = "all") -> DictConfig:
    """
    Carga la configuración con merge jerárquico:
        structured defaults -> base.yaml -> env/<env>.yaml -> tenants/<tenant>.yaml

    Args:
        env:    Ambiente destino ('dev', 'qa', 'main').
        tenant: Código de tenant en minúscula ('sv', 'hn', …) o 'all'.

    Returns:
        DictConfig fusionado, con todas las claves validadas contra PipelineConfig.

    Raises:
        FileNotFoundError: Si base.yaml o env/<env>.yaml no existen.
        ValueError:        Si el código de ambiente es inválido.
    """
    valid_envs = {"dev", "qa", "main", "dbx"}
    if env not in valid_envs:
        raise ValueError(f"Ambiente '{env}' no válido. Usar: {valid_envs}")

    # 1. Schema estructurado como base (proporciona tipos y defaults)
    schema: DictConfig = OmegaConf.structured(PipelineConfig)

    # 2. base.yaml — defaults compartidos
    base_path = _CONFIG_DIR / "base.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"Archivo de configuración base no encontrado: {base_path}")
    base_cfg = OmegaConf.load(base_path)

    # 3. env/<env>.yaml — overrides por ambiente
    env_path = _CONFIG_DIR / "env" / f"{env}.yaml"
    if not env_path.exists():
        raise FileNotFoundError(f"Config de ambiente no encontrada: {env_path}")
    env_cfg = OmegaConf.load(env_path)

    merged = OmegaConf.merge(schema, base_cfg, env_cfg)

    # 4. tenants/<tenant>.yaml — overrides por tenant (opcional)
    if tenant and tenant != "all":
        tenant_path = _CONFIG_DIR / "tenants" / f"{tenant}.yaml"
        if tenant_path.exists():
            tenant_cfg = OmegaConf.load(tenant_path)
            merged = OmegaConf.merge(merged, tenant_cfg)
            logger.debug("Tenant config cargada: %s", tenant_path)
        else:
            logger.warning(
                "No se encontró config para tenant '%s' en %s. Continuando con defaults.",
                tenant,
                tenant_path,
            )

    logger.debug("Config final: %s", OmegaConf.to_yaml(merged))
    return merged


def list_configured_tenants() -> list[str]:
    """
    Devuelve los códigos de tenant para los que existe un archivo de config.

    Returns:
        Lista de strings en minúscula (ej: ['sv', 'hn', 'gt', 'ni', 'cr', 'pa']).
    """
    tenants_dir = _CONFIG_DIR / "tenants"
    if not tenants_dir.exists():
        return []
    return sorted(f.stem for f in tenants_dir.glob("*.yaml"))


# Helpers de composición de paths (centralizados para evitar f-strings dispersos)


def bronze_path(cfg: DictConfig, tenant: str, table: str) -> str:
    """data/bronze/<tenant>/<table>"""
    return f"{cfg.paths.bronze}/{tenant}/{table}"


def silver_path(cfg: DictConfig, tenant: str, table: str) -> str:
    """data/silver/<tenant>/<table>"""
    return f"{cfg.paths.silver}/{tenant}/{table}"


def gold_path(cfg: DictConfig, tenant: str, table: str) -> str:
    """data/gold/<tenant>/<table>"""
    return f"{cfg.paths.gold}/{tenant}/{table}"


def quarantine_path(cfg: DictConfig, layer: str, tenant: str, table: str) -> str:
    """data/<layer>_quarantine/<tenant>/<table>"""
    return f"{cfg.paths.quarantine_root}/{layer}_quarantine/{tenant}/{table}"


def raw_file_path(cfg: DictConfig, filename: str) -> str:
    """data/raw/<filename>"""
    return f"{cfg.paths.raw}/{filename}"
