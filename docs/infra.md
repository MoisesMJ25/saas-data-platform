# Infraestructura — Onboarding de un Tenant en Unity Catalog + ADLS Gen2

Este documento describe qué recursos provisionaría Terraform para agregar un nuevo tenant
a la plataforma SAAS en producción (Databricks + ADLS Gen2 + Unity Catalog).

---

## Recursos que provisionaría Terraform

Para onboardear un tenant `cr` (Costa Rica) al ambiente `prod`, Terraform crea:

| Recurso | Descripción |
|---------|-------------|
| `azurerm_storage_container` | Contenedor ADLS Gen2 dedicado al tenant (o prefijo dentro del contenedor compartido) |
| `databricks_schema` × 4 | Schemas `bronze_cr`, `silver_cr`, `gold_cr`, `bronze_quarantine_cr` en el catálogo `saas_prod` |
| `databricks_grants` | Permisos de lectura/escritura para los service principals del pipeline |
| `databricks_secret_scope` | Scope de secretos del tenant (credentials de fuentes, keys de ADLS) |
| `databricks_secret` | Secretos individuales (storage account key, SPN client secret) |
| `databricks_external_location` | Mapeo entre ADLS path y Unity Catalog para las tablas del tenant |

---

## Snippet Terraform — módulo `tenant_onboarding`

```hcl
# modules/tenant_onboarding/variables.tf

variable "tenant_code" {
  description = "Código de tenant en minúscula (ej: 'cr', 'sv', 'hn')"
  type        = string
  validation {
    condition     = can(regex("^[a-z]{2}$", var.tenant_code))
    error_message = "tenant_code debe ser exactamente 2 letras minúsculas."
  }
}

variable "environment" {
  description = "Ambiente destino: dev | qa | prod"
  type        = string
}

variable "unity_catalog_name" {
  description = "Nombre del catálogo Unity Catalog (ej: saas_prod)"
  type        = string
}

variable "adls_storage_account" {
  description = "Nombre de la cuenta de almacenamiento ADLS Gen2"
  type        = string
}

variable "pipeline_principal_id" {
  description = "Object ID del service principal que ejecuta el pipeline"
  type        = string
}

# modules/tenant_onboarding/main.tf

locals {
  layers = ["bronze", "silver", "gold", "bronze_quarantine", "silver_quarantine"]
  schema_names = {
    for layer in local.layers :
    layer => "${layer}_${var.tenant_code}"
  }
  adls_base_path = "abfss://data@${var.adls_storage_account}.dfs.core.windows.net"
}

# Schemas en Unity Catalog — uno por capa por tenant
resource "databricks_schema" "tenant_schemas" {
  for_each     = local.schema_names
  catalog_name = var.unity_catalog_name
  name         = each.value
  comment      = "Capa ${each.key} del tenant ${var.tenant_code} (${var.environment})"
}

# Grants de escritura al pipeline principal
resource "databricks_grants" "pipeline_grants" {
  for_each = databricks_schema.tenant_schemas

  schema = "${var.unity_catalog_name}.${each.value.name}"

  grant {
    principal  = var.pipeline_principal_id
    privileges = ["CREATE TABLE", "MODIFY", "SELECT"]
  }
}

# External location para cada capa (mapea ADLS path a Unity Catalog)
resource "databricks_external_location" "tenant_locations" {
  for_each        = local.schema_names
  name            = "${var.environment}_${each.value}"
  url             = "${local.adls_base_path}/${each.key}/${var.tenant_code}"
  credential_name = "saas_storage_credential"
  comment         = "Path ADLS para ${each.value} en ${var.environment}"
}

# Secret scope del tenant (para credenciales específicas del tenant si las hay)
resource "databricks_secret_scope" "tenant_scope" {
  name = "saas-${var.environment}-${var.tenant_code}"
}

# outputs
output "schema_ids" {
  value = { for k, v in databricks_schema.tenant_schemas : k => v.id }
}
```

### Uso del módulo

```hcl
# environments/prod/main.tf

module "tenant_cr" {
  source = "../../modules/tenant_onboarding"

  tenant_code           = "cr"
  environment           = "prod"
  unity_catalog_name    = "saas_prod"
  adls_storage_account  = "saasprodadls"
  pipeline_principal_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

---

## Notas de diseño

- El módulo es **idempotente**: aplicarlo dos veces sobre el mismo tenant no crea recursos duplicados.
- Terraform state está centralizado en un backend de Azure Storage (`azurerm` backend).
- Un nuevo tenant solo requiere agregar un bloque `module "tenant_<código>"` en el archivo
  del ambiente correspondiente, más el archivo `config/tenants/<código>.yaml` en el repositorio
  del pipeline.
- El snippet asume que el catálogo Unity y el storage credential ya existen (gestionados
  por un módulo de bootstrap de plataforma separado).
