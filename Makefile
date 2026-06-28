# SAAS Data Platform — Makefile
# Uso: make <target>   |   Requiere Python 3.11+ y uv instalado


PYTHON   := python3.11
UV       := uv
VENV     := .venv
SRC_DIR  := src
TEST_DIR := tests

.PHONY: help install install-dev lint format test test-unit test-integration \
        run-dev run-all clean check-versions

# -- Ayuda
help:  ## Muestra este mensaje de ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# -- Entorno
install:  ## Instala dependencias de producción con uv
	$(UV) venv $(VENV) --python $(PYTHON)
	$(UV) pip install -e . --python $(VENV)/bin/python

install-dev:  ## Instala dependencias de desarrollo (lint, tests, mypy)
	$(UV) venv $(VENV) --python $(PYTHON)
	$(UV) pip install -e ".[dev]" --python $(VENV)/bin/python
	@echo "Entorno dev listo en $(VENV)/"

check-versions:  ## Verifica versiones exactas instaladas
	@$(VENV)/bin/python -c "import pyspark; print('PySpark:', pyspark.__version__)"
	@$(VENV)/bin/python -c "import delta; print('delta-spark:', delta.__version__)"
	@$(VENV)/bin/python -c "import omegaconf; print('OmegaConf:', omegaconf.__version__)"
	@$(VENV)/bin/python --version

# -- Calidad de código
lint:  ## Ejecuta ruff (linter PEP8 + bugs)
	$(VENV)/bin/ruff check $(SRC_DIR) $(TEST_DIR) mentoring/good_code.py

format:  ## Formatea código con ruff
	$(VENV)/bin/ruff format $(SRC_DIR) $(TEST_DIR)

format-check:  ## Verifica formato sin modificar (modo CI)
	$(VENV)/bin/ruff format --check $(SRC_DIR) $(TEST_DIR)

# -- Tests
test:  ## Corre todos los tests con cobertura
	$(VENV)/bin/pytest $(TEST_DIR) --cov=$(SRC_DIR)/saas_pipeline --cov-report=term-missing

test-unit:  ## Solo tests unitarios (sin Spark, rápidos)
	$(VENV)/bin/pytest $(TEST_DIR) -m unit

test-integration:  ## Tests de integración con SparkSession local
	$(VENV)/bin/pytest $(TEST_DIR) -m integration

# -- Pipeline
run-dev:  ## Corre el pipeline completo en dev para tenant SV
	$(VENV)/bin/python -m saas_pipeline.cli run \
	  --env dev \
	  --tenant sv \
	  --start-date 2025-01-01 \
	  --end-date   2025-06-30

run-all:  ## Corre el pipeline para todos los tenants en dev
	$(VENV)/bin/python -m saas_pipeline.cli run \
	  --env dev \
	  --tenant all \
	  --start-date 2025-01-01 \
	  --end-date   2025-06-30

# -- Limpieza
clean:  ## Elimina artefactos generados (no datos)
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name "*.egg-info"   -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	@echo "Limpieza completada"