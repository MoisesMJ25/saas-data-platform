"""
spark.py — Construcción centralizada de la SparkSession con soporte Delta Lake.

Usar configure_spark_with_delta_pip (delta-spark) en lugar de configurar el
JAR manualmente permite que el mismo código funcione en local (pip) y en
Databricks (Delta ya está disponible en el runtime, getOrCreate() lo reutiliza).
"""
from __future__ import annotations

import importlib.metadata
import logging
import os
import subprocess
from pathlib import Path

from delta import configure_spark_with_delta_pip
from omegaconf import DictConfig
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------
# Windows: rutas con caracteres no-ASCII (é) corrompen el classpath que
# construye spark-class2.cmd. Se fija SPARK_HOME a la ruta corta antes
# del primer getOrCreate(). En Linux/Mac es un no-op.

if os.name == "nt" and "SPARK_HOME" not in os.environ:
    try:
        import pyspark as _pyspark
        _home = os.path.dirname(_pyspark.__file__)
        if any(ord(c) > 127 for c in _home):
            _ps = (
                f"(New-Object -ComObject Scripting.FileSystemObject)"
                f".GetFolder('{_home}').ShortPath"
            )
            _r = subprocess.run(
                ["powershell", "-NoProfile", "-Command", _ps],
                capture_output=True, text=True, timeout=10,
            )
            if _r.returncode == 0 and _r.stdout.strip():
                os.environ["SPARK_HOME"] = _r.stdout.strip()
    except Exception:
        pass

logger = logging.getLogger(__name__)


def _short_path_win(path: str) -> str:
    """Devuelve la ruta corta 8.3 de Windows; si falla devuelve la original."""
    ps = f'(New-Object -ComObject Scripting.FileSystemObject).GetFile("{path}").ShortPath'
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return path


def _delta_jars_from_ivy_cache() -> str | None:
    """
    En Windows con paths no-ASCII, configure_spark_with_delta_pip añade los JARs
    vía spark.jars.packages (Ivy), lo que desencadena SparkContext.addFile() y
    requiere winutils.exe. Este helper devuelve los JARs ya cacheados como
    rutas cortas 8.3 para usar en spark.jars (sin Ivy, sin winutils).

    Devuelve None si no aplica el workaround (Linux/Mac o paths ASCII) o si
    los JARs aún no están cacheados.
    """
    if os.name != "nt":
        return None
    home = str(Path.home())
    if not any(ord(c) > 127 for c in home):
        return None

    try:
        delta_version = importlib.metadata.version("delta_spark")
    except Exception:
        return None

    ivy_dir = Path.home() / ".ivy2" / "jars"
    if not ivy_dir.exists():
        return None

    candidates = [
        ivy_dir / f"io.delta_delta-spark_2.12-{delta_version}.jar",
        ivy_dir / f"io.delta_delta-storage-{delta_version}.jar",
    ]
    antlr = sorted(ivy_dir.glob("org.antlr_antlr4-runtime-*.jar"))
    if antlr:
        candidates.append(antlr[0])

    if not all(p.exists() for p in candidates):
        return None  # aún no descargados; usar configure_spark_with_delta_pip primero

    return ",".join(_short_path_win(str(p)) for p in candidates)


def get_spark_session(cfg: DictConfig) -> SparkSession:
    """
    Construye o recupera la SparkSession configurada para Delta Lake.

    Idempotente: si ya existe una sesión activa con el mismo appName,
    getOrCreate() la devuelve sin crear una nueva. En Databricks, la sesión
    del cluster ya existe; master y appName son ignorados.

    Args:
        cfg: DictConfig fusionado con clave 'spark' (SparkConfig).

    Returns:
        SparkSession lista para leer/escribir Delta.
    """
    builder = (
        SparkSession.builder
        .appName(cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    local_jars = _delta_jars_from_ivy_cache()
    if local_jars:
        # Windows con ruta no-ASCII: usar JARs locales en lugar de Ivy para
        # evitar la dependencia en winutils.exe (SparkContext.addFile -> chmod).
        builder = builder.config("spark.jars", local_jars)
        spark = builder.getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel(cfg.spark.log_level)

    logger.info(
        "SparkSession activa | app=%s | master=%s | shuffle_partitions=%s",
        cfg.spark.app_name,
        cfg.spark.master,
        cfg.spark.shuffle_partitions,
    )
    return spark
