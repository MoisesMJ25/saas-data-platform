# SAAS Data Platform — Pipeline Medallion Multi-Tenant

Pipeline de datos **Bronze -> Silver -> Gold** con aislamiento multi-tenant sobre
PySpark + Delta Lake, siguiendo la arquitectura del Proyecto SAAS (Apex Digital / M5).

---

## Requisitos del entorno

| Componente | Versión mínima |
|------------|---------------|
| Python     | 3.11          |
| Java       | 11 o 17       |
| PySpark    | 3.5.3         |
| delta-spark | 3.2.0        |

> **Java:** requerido por PySpark. Configurar `JAVA_HOME` antes de ejecutar.
> En Windows con IntelliJ/PyCharm, el JDK bundled del IDE es suficiente.

---

## Setup del entorno

```bash
# 1. Clonar el repositorio
git clone <repo-url>
cd saas-data-platform

# 2. Crear virtualenv e instalar dependencias
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
.venv\Scripts\activate             # Windows

pip install -e ".[dev]"

# 3. Verificar versiones instaladas
python -c "import pyspark; print('PySpark', pyspark.__version__)"
python -c "import delta; print('delta-spark', delta.__version__)"
```

Con `uv` (alternativa más rápida):

```bash
uv venv .venv --python 3.11
uv pip install -e ".[dev]"
```

---

## Estructura del repositorio

```
saas-data-platform/
├── README.md
├── Makefile                         # Targets: install-dev, lint, test, run-dev
├── pyproject.toml                   # Dependencias, ruff, pytest
├── .github/workflows/ci.yml         # CI: lint + tests en push/PR
│
├── config/
│   ├── base.yaml                    # Defaults compartidos
│   ├── env/
│   │   ├── dev.yaml                 # Paths locales, 4 shuffle partitions
│   │   ├── qa.yaml
│   │   └── main.yaml
│   └── tenants/
│       ├── sv.yaml  hn.yaml  gt.yaml
│       ├── ec.yaml  jm.yaml  pe.yaml
│
├── data/                            # Generado en ejecución. No versionar.
│   ├── raw/                         # CSVs de entrada
│   ├── bronze/<tenant>/<table>/fecha_proceso=YYYYMMDD/
│   ├── silver/<tenant>/<table>/fecha_proceso=YYYYMMDD/
│   ├── gold/<tenant>/<table>/
│   ├── bronze_quarantine/<tenant>/<table>/
│   ├── silver_quarantine/<tenant>/<table>/
│   └── shared/quality_logs/
│
├── src/saas_pipeline/
│   ├── config.py                    # Carga OmegaConf jerárquica + helpers de paths
│   ├── spark.py                     # SparkSession con Delta configurado
│   ├── bronze.py                    # Ingesta CSV -> Delta, cuarentena de fechas
│   ├── silver.py                    # dim_materials SCD2 + fact_deliveries MERGE
│   ├── gold.py                      # daily_metrics_by_delivery_type
│   ├── quality.py                   # Validaciones + escritura en quality_logs
│   └── cli.py                       # Punto de entrada Click (saas-pipeline)
│
├── tests/
│   ├── test_silver_transforms.py    # 17 tests: normalización, filtrado, cuarentena, join
│   └── test_quality.py              # 11 tests: log rows, checks críticos, severidades
│
├── mentoring/
│   ├── bad_code.py                  # Código hipotético del junior (sin modificar)
│   ├── good_code.py                 # Refactoring producción-ready
│   └── code_review.md              # 4+ observaciones accionables + nota al junior
│
└── docs/
    ├── observations.md              # 4 observaciones sustantivas a la arquitectura
    ├── infra.md                     # Terraform snippet para onboarding de tenant
    └── onboarding-tenant.md         # Guía paso a paso de onboarding
```

---

## Ejecutar el pipeline

### Pipeline completo (Bronze -> Silver -> Gold)

```bash
# Un tenant específico
saas-pipeline run --env dev --tenant sv --start-date 2025-01-01 --end-date 2025-06-30

# Todos los tenants configurados
saas-pipeline run --env dev --tenant all

# Con rango de fechas acotado
saas-pipeline run --env dev --tenant sv --start-date 2025-01-01 --end-date 2025-03-31
```

### Solo Bronze

```bash
saas-pipeline bronze --env dev --tenant sv
```

### Con Python directamente

```bash
python -m saas_pipeline.cli run --env dev --tenant sv
```

---

## Tests y linter

```bash
# Todos los tests
pytest tests/ -v

# Linter (PEP8 + bugs)
ruff check src/ tests/

# Verificar formato
ruff format --check src/ tests/

# Con Makefile
make test
make lint
```

Los tests son unitarios (no requieren Delta en disco): utilizan DataFrames in-memory
y SparkSession local. Tiempo estimado: 2-4 minutos (incluye arranque de JVM).

---

## Inspección de datos (desarrollo local)

`inspect_data.py` en la raíz del proyecto es un script utilitario para leer y
explorar los parquets generados por el pipeline sin necesidad de un notebook ni
de levantar una sesión Spark manualmente.

**Uso:** edita las 4 variables al inicio del archivo y presiona **Run** en el IDE
o ejecútalo desde la terminal:

```bash
python inspect_data.py
```

```python
# inspect_data.py — variables de configuración
LAYER  = "bronze"      # bronze | silver | gold | bronze_quarantine
TENANT = "sv"          # sv | hn | gt | ni | cr | pa
TABLE  = "deliveries"  # deliveries | fact_deliveries | dim_materials | daily_metrics_by_delivery_type
DATE   = "20250107"    # YYYYMMDD para un día específico — None lee toda la tabla
```

El script reutiliza `get_spark_session` del pipeline (misma sesión, mismos JARs
Delta) e imprime el esquema, el conteo de filas y las primeras 10 filas de la
partición seleccionada.

---

## Onboarding de un tenant nuevo

Para agregar un nuevo tenant al pipeline basta con:

1. **Crear el archivo de config del tenant:**

   ```bash
   # config/tenants/<código>.yaml
   tenant:
     code:         "cr"
     display_name: "Costa Rica"
     country_code: "CR"
   ```

2. **Asegurarse de que los datos estén en el CSV fuente** con `pais = 'CR'` (el pipeline
   normaliza a minúscula automáticamente).

3. **Ejecutar el pipeline con el nuevo tenant:**

   ```bash
   saas-pipeline run --env dev --tenant cr --start-date 2025-01-01 --end-date 2025-06-30
   ```

El pipeline crea automáticamente las rutas de Bronze, Silver, Gold y cuarentena bajo
`data/<layer>/cr/`. No se require ningún cambio de código.

Para el onboarding completo en producción (Unity Catalog + ADLS), ver
[`docs/onboarding-tenant.md`](docs/onboarding-tenant.md) y el snippet Terraform en
[`docs/infra.md`](docs/infra.md).

---

## Configuración jerárquica

La configuración sigue la jerarquía (sección 5.8 de la arquitectura):

```
base.yaml -> env/<env>.yaml -> tenants/<tenant>.yaml
```

Cada nivel sobreescribe el anterior. Los parámetros disponibles:

| Parámetro | Descripción |
|-----------|-------------|
| `paths.bronze / silver / gold` | Rutas base por capa |
| `paths.quarantine_root` | Raíz para tablas de cuarentena |
| `paths.quality_logs` | Ruta de la tabla Delta de quality logs |
| `execution.start_date / end_date` | Rango de `fecha_proceso` a procesar |
| `execution.tenant` | Código de tenant o `"all"` |
| `execution.fail_fast` | Si `true`, aborta al primer fallo de tenant |
| `quality.fail_on_critical` | Si `true`, aborta antes de Gold si hay check crítico |

---

## Manejo de anomalías

| Tipo | Acción | Destino |
|------|--------|---------|
| `fecha_proceso` nula o formato inválido | Cuarentena | `bronze_quarantine/<tenant>/deliveries/` |
| `cantidad` nula, negativa o cero | Cuarentena | `silver_quarantine/<tenant>/fact_deliveries/` |
| `precio` nulo | Cuarentena | `silver_quarantine/<tenant>/fact_deliveries/` |
| `material` sin match en catálogo para la fecha | Cuarentena | `silver_quarantine/<tenant>/fact_deliveries/` |
| `tipo_entrega` fuera de ZPRE/ZVE1/Z04/Z05 | Descarte | Contabilizado en logs, no persistido |
| Duplicado exacto | Deduplicación | Se conserva una copia, se descarta el resto |

---

## Qué dejé fuera y por qué

| Ítem | Decisión | Razón |
|------|----------|-------|
| Auto Loader / Streaming | No implementado | Marcado como "Provisto en arquitectura" y como bonus opcional. El foco fue el pipeline batch parametrizado. |
| Segunda tabla Gold | No implementado | Con el tiempo disponible, prioricé la corrección de la tabla principal (`daily_metrics_by_delivery_type`) sobre añadir una segunda tabla más simple. |
| Dashboard (Streamlit / Databricks SQL) | No implementado | Bonus opcional; el alcance base es el prioritario. |
| Pre-commit hooks | No implementado | El linter y tests están en CI; los hooks locales son complementarios. |
| `terraform validate` funcional | Solo snippet ilustrativo | La prueba especifica explícitamente un snippet de ~30-50 líneas sin requerir que valide contra una cuenta real. |
| Tests de integración end-to-end (Bronze->Silver->Gold con Delta en disco) | No implementados | Los tests unitarios cubren la lógica de negocio; los tests e2e requerirían gestión de paths temporales. Serían el siguiente paso natural. |

---

## Versiones exactas instaladas

```
Python      3.11.x
PySpark     3.5.3
delta-spark 3.2.0
OmegaConf   2.3.0
click       8.x
ruff        0.5+
pytest      8.2+
```

Verificar con:

```bash
make check-versions
```
