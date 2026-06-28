# Onboarding de un Tenant Nuevo

Guía paso a paso para agregar una nueva unidad de negocio (tenant) al pipeline SAAS.
El proceso está diseñado para ser ejecutado en < 15 minutos sin cambios de código.

---

## Paso 1 — Crear el archivo de configuración del tenant

Crear `config/tenants/<código>.yaml` con la metadata del tenant:

```yaml
# config/tenants/cr.yaml
tenant:
  code:         "cr"
  display_name: "Costa Rica"
  country_code: "CR"
```

El código debe ser la versión en minúscula del valor del campo `pais` en el CSV de entregas.

**Parámetros opcionales de override** (si el tenant necesita rutas distintas o config Spark diferente):

```yaml
# Solo incluir si difieren del base.yaml
paths:
  bronze: "abfss://data@myadls.dfs.core.windows.net/bronze"

spark:
  shuffle_partitions: 16   # Si el tenant tiene más volumen
```

---

## Paso 2 — Verificar que los datos existen en el CSV fuente

```bash
python -c "
import csv
tenants = set(r['pais'].lower() for r in csv.DictReader(open('data/raw/global_mobility_data_entrega_productos.csv')))
print('Tenants en CSV:', sorted(tenants))
"
```

El código del nuevo tenant debe aparecer en el resultado.

---

## Paso 3 — (Producción) Provisionar infraestructura con Terraform

```bash
cd environments/prod
terraform plan -target=module.tenant_cr
terraform apply -target=module.tenant_cr
```

Ver [`docs/infra.md`](infra.md) para el detalle del módulo Terraform.

En **local/dev**, no se requiere Terraform: el pipeline crea los paths `data/<layer>/cr/`
automáticamente en la primera ejecución.

---

## Paso 4 — Ejecutar el pipeline para el nuevo tenant

```bash
# Primera carga completa (Bronze + Silver + Gold)
saas-pipeline run \
  --env dev \
  --tenant cr \
  --start-date 2025-01-01 \
  --end-date   2025-06-30
```

El pipeline crea automáticamente:

```
data/bronze/cr/deliveries/fecha_proceso=20250115/
data/silver/cr/dim_materials/
data/silver/cr/fact_deliveries/fecha_proceso=20250115/
data/gold/cr/daily_metrics_by_delivery_type/
data/shared/quality_logs/           ← se añaden los checks del nuevo tenant
```

---

## Paso 5 — Verificar los quality logs

```bash
python -c "
from saas_pipeline.config import load_config
from saas_pipeline.spark import get_spark_session

cfg   = load_config('dev', 'cr')
spark = get_spark_session(cfg)
spark.read.format('delta').load(cfg.paths.quality_logs) \
     .filter('tenant_id = \"cr\"') \
     .show(truncate=False)
spark.stop()
"
```

Todos los checks deben aparecer con `check_passed = true`. Si alguno falla, revisar
las tablas de cuarentena correspondientes.

---

## Paso 6 — (Opcional) Correr `--tenant all` para validar integración

```bash
saas-pipeline run --env dev --tenant all --start-date 2025-04-01 --end-date 2025-06-30
```

Verifica que el nuevo tenant coexiste correctamente con los demás sin afectar sus datos.

---

## Resumen del contrato de onboarding

| Requisito | Detalle |
|-----------|---------|
| Archivo de config | `config/tenants/<código>.yaml` con `tenant.code` en minúscula |
| Datos en CSV | Campo `pais` debe coincidir con el código (pipeline normaliza a minúscula) |
| Infra (prod) | Módulo Terraform `tenant_onboarding` provisionado |
| Cambios de código | **Ninguno** — el pipeline es data-driven por diseño |
