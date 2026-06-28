"""
inspect_data.py — Visor rápido de parquets del proyecto.
Ajusta las 4 variables de CONFIG.
"""
from pathlib import Path

from omegaconf import OmegaConf

from src.saas_pipeline.config import PipelineConfig
from src.saas_pipeline.spark import get_spark_session


# CONFIG — para revisar .parquets

LAYER  = "gold"
TENANT = "hn"
TABLE  = "daily_metrics_by_delivery_type"
DATE   = "20250107" # None

ROOT = Path(__file__).parent


def main() -> None:
    cfg = OmegaConf.structured(PipelineConfig)
    spark = get_spark_session(cfg)

    path = ROOT / "data" / LAYER / TENANT / TABLE
    if DATE:
        path = path / f"fecha_proceso={DATE}"

    print(f"\n---> {path}\n")
    df = spark.read.parquet(str(path))
    df.printSchema()
    print(f"Filas: {df.count()}\n")
    df.show(10, truncate=False)


if __name__ == "__main__":
    main()
