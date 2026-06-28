# Observaciones a la Arquitectura — Proyecto SAAS

> **Propósito:** Este documento registra, en el espíritu definido en la sección 9.2 de la prueba,
> las decisiones de la arquitectura provista con las que se discrepa, las áreas de ambigüedad
> resueltas durante la implementación y las mejoras propuestas para Horizonte 2-3.
> Los puntos aquí documentados **no fueron cambiados unilateralmente** en el código;
> se implementó la arquitectura tal como se describió y estas observaciones quedan
> para discusión en la sustentación.

---

## OBS-01 — `dim_materials` es un catálogo compartido modelado como tabla por tenant; la duplicación es innecesaria y dificulta la evolución del catálogo

### Decisión de la arquitectura provista

El naming en Unity Catalog asigna `dim_materials` bajo el schema del tenant:
`saas_dev.silver_sv.dim_materials`, lo que implica una tabla Delta por tenant en la simulación local:
`data/silver/sv/dim_materials/`, `data/silver/hn/dim_materials/`, etc.

### Punto de desacuerdo

El catálogo de materiales (`materials_catalog.csv`) es **compartido**: los mismos SKUs se usan
en todos los tenants. Modelarlo por tenant implica:

1. **Duplicación de datos:** La misma tabla SCD Type 2 se escribe N veces (una por tenant).
   Con 6 tenants el tamaño es 6×; con 60 tenants, 60×. A medida que el catálogo crece esto
   se convierte en un problema de almacenamiento y de consistencia eventual entre copias.
2. **Riesgo de divergencia:** Si el proceso de carga de un tenant falla a mitad del catálogo,
   ese tenant tendrá una versión distinta del catálogo que el resto. Las métricas cross-tenant
   en Gold dejarían de ser comparables.
3. **Versionado de SCD Type 2 incoherente:** Si se añade una nueva versión de un SKU al CSV,
   todos los tenants deben procesarlo de forma coordinada. No existe mecanismo en la arquitectura
   actual para garantizar esa sincronía.

### Propuesta alternativa

Un schema `shared` dentro del catálogo por ambiente para datos de referencia cross-tenant:

```
Unity Catalog: saas_dev.shared.dim_materials
Local path:    data/silver/shared/dim_materials/
```

El join en `fact_deliveries` leería desde `shared.dim_materials`, no desde la tabla del tenant.

**Trade-offs:**
- (+) Una sola fuente de verdad para el catálogo; cero duplicación.
- (+) El SCD Type 2 se gestiona una sola vez; la coherencia cross-tenant es trivial.
- (-) Requiere grants de lectura cross-schema en Unity Catalog (`GRANT SELECT ON shared.dim_materials TO silver_sv_job`).
- (-) Si un tenant necesita un catálogo customizado en el futuro (precios regionales), el modelo
  compartido requiere añadir una clave de tenant al SCD Type 2, lo que lo hace más complejo.

**Resolución en esta implementación:** Se implementó `dim_materials` por tenant siguiendo el naming
de la arquitectura provista. En `silver.py`, `process_dim_materials` escribe el catálogo completo
bajo la ruta del tenant procesado.

---

## OBS-02 — Estrategia de particionamiento por `fecha_proceso` vs. particionamiento jerárquico (`year/month/day`)

### Decisión de la arquitectura provista

El diseño especifica el uso de un nivel único de particionamiento en las capas Bronze y Silver mediante la convención:
`data/{capa}/{tenant}/{entidad}/fecha_proceso=YYYYMMDD/`

### Punto de desacuerdo

Aunque la estructura de `fecha_proceso` es funcional para un MVP y facilita la atomicidad de los batch de carga,
presenta limitaciones técnicas a medida que el Data Lake crece:

1. **Eficiencia en el Partition Pruning:** Al tener todos los datos de un mismo día bajo una única clave,
    las consultas que requieren rangos de tiempo amplios (ej. reportes mensuales o anuales)
    obligan al motor de cómputo (Spark/SQL) a realizar un escaneo más costoso o a manejar una cantidad masiva de subcarpetas
    en un solo nivel, impactando el rendimiento del metastore.
2. **Escalabilidad del sistema de archivos:** En volúmenes de datos de escala Petabyte,
    el particionamiento plano por fecha (`YYYYMMDD`) puede exceder los límites de rendimiento de las interfaces de sistemas
    de archivos sobre los que se apoya ADLS Gen2 al intentar listar o filtrar particiones específicas.

### Propuesta alternativa

Implementar un particionamiento jerárquico estandarizado para Data Lakes de alto rendimiento:

**Trade-offs:**
- (+) **Partition Pruning superior:** Permite al motor de consultas descartar rápidamente años o meses completos
    antes de acceder al nivel de detalle del día.
- (+) **Mejor organización:** Reduce la cantidad de subcarpetas directas en cada nivel, facilitando la gestión y el mantenimiento a largo plazo.
- (-) **Complejidad de implementación:** Requiere una lógica adicional en el `write.partitionBy()` del pipeline de Spark para extraer los componentes de fecha.
- (-) **Mantenibilidad:** La estructura es más profunda, lo que puede requerir scripts de limpieza (retención) más complejos.

**Resolución en esta implementación:** Se ha implementado el particionamiento bajo la convención `fecha_proceso=YYYYMMDD`
    según lo definido originalmente en la arquitectura de la prueba, priorizando la adherencia a los estándares solicitados para el MVP.
    Esta observación se documenta como una oportunidad de mejora arquitectónica para la fase de producción o escalado a gran escala.

---

## OBS-03 — Desacoplamiento de la capa Bronze mediante herramientas de orquestación externas (ADF)

### Decisión de la arquitectura provista
La arquitectura actual propone un pipeline unificado de extremo a extremo (Bronze, Silver, Gold) ejecutado íntegramente sobre Databricks.

### Punto de desacuerdo / Consideración de optimización
Si bien un pipeline unificado simplifica la gestión del código, en entornos con gran variedad de fuentes (multi-tenant) y volumen de datos,
centralizar la ingesta en Databricks puede resultar ineficiente desde la perspectiva de costos y gestión de conectores.

1. **Eficiencia de Costos:** Las actividades de ingesta "Raw" (Copy Data) no requieren la capacidad de cómputo
    de un clúster de Spark. Delegar esta tarea a un servicio de orquestación (ej. Azure Data Factory)
    permite reducir el tiempo de encendido de clústeres, optimizando el costo operativo.
2. **Escalabilidad y Mantenimiento:** Un enfoque desacoplado permite gestionar configuraciones de ingesta externas (metadata-driven)
    y manejar reintentos, validaciones de conectividad y logs de transferencia de forma nativa fuera del código de transformación.

### Propuesta alternativa
Adoptar un patrón de arquitectura donde la capa Bronze sea poblada por un orquestador (ADF/Pipeline), dejando que Databricks
comience su ciclo de vida desde la capa Bronze hacia la Silver mediante disparadores de eventos (File Arrival Trigger).

**Trade-offs:**
- (+) **Costos optimizados:** Se utiliza el recurso adecuado para la tarea (Ingesta: ADF / Procesamiento: Databricks).
- (+) **Gobierno de datos:** Facilita la implementación de una capa de "Landing" estandarizada independiente de la lógica de negocio de transformación.
- (-) **Complejidad de orquestación:** Requiere gestionar dos plataformas distintas y coordinar dependencias entre ellas.

**Resolución en esta implementación:** Se ha mantenido la ingesta en Databricks para cumplir con la restricción del alcance de la prueba.
    No obstante, se recomienda para una fase de producción evaluar la migración de los procesos de ingesta hacia un esquema
    de orquestación por metadatos utilizando herramientas nativas de Azure, delegando a Databricks
    solo las tareas de alta carga de cómputo analítico.

---

## OBS-04 — Centralización de Observabilidad y Alertas Proactivas

### Decisión de la arquitectura actual
El pipeline implementado cuenta con una lógica robusta de manejo de excepciones mediante `try-except` y flags
de control (`fail_fast`, `fail_on_critical`). Los errores se capturan, loguean y reportan en la ejecución del CLI,
permitiendo una gestión resiliente del ciclo de vida de los datos.

### Punto de desacuerdo / Consideración de optimización
Aunque el manejo interno de errores es correcto para la ejecución local, la visibilidad es pasiva.
Los logs residen únicamente en la ejecución del proceso; ante un fallo, un operador no se enteraría a menos que
revise manualmente la salida. En un ecosistema corporativo multi-tenant, la falta de alertas proactivas aumenta el tiempo
de respuesta (MTTR) ante incidencias críticas.

### Propuesta alternativa
Evolucionar de un manejo de errores local a una estrategia de observabilidad y notificación externa:

1. **Integración con Orquestador (e.g., Azure Data Factory):** Ejecutar el pipeline de Databricks desde un orquestador
    que capture el estado de finalización. Si el pipeline termina con error, el orquestador dispara automáticamente
    una alerta (Webhook) a canales de comunicación corporativos (Teams/Slack).
2. **Telemetry Sink:** Centralizar los logs generados por el pipeline y los errores capturados en un workspace de **Azure Log Analytics**.
3. **Dashboards de Monitoreo:** Implementar dashboards (Databricks SQL o PowerBI) que consuman los logs centralizados
    para visualizar el *health status* de cada tenant en tiempo real.

**Trade-offs:**
- (+) **Transparencia total:** Alertas instantáneas permiten acciones correctivas proactivas.
- (+) **Auditabilidad:** Los errores quedan registrados en un sistema externo, facilitando el análisis de patrones de fallo recurrentes.
- (-) **Carga administrativa:** Requiere configuración y mantenimiento de servicios de monitoreo adicionales
    en Azure (Log Analytics, Azure Monitor).

**Resolución en esta implementación:** Se ha implementado un manejo de excepciones resiliente dentro del CLI que permite
la ejecución parcial y el registro detallado de logs. Se documenta esta observación para justificar que,
en una fase de despliegue productivo, este mecanismo debe conectarse a una capa de observabilidad externa para garantizar
el cumplimiento de los niveles de servicio (SLA) de la plataforma.

---

## OBS-05 — Acoplamiento de esquemas (Hardcoded) vs. Ingesta basada en Metadata

### Decisión de la arquitectura actual
El pipeline actual utiliza esquemas definidos explícitamente (`StructType`) en el código fuente (`bronze.py`).
Esta decisión garantiza la integridad y calidad de los datos desde la ingesta, evitando la inferencia automática
que suele ser costosa y propensa a errores en entornos de producción.

### Punto de desacuerdo / Consideración de optimización
El uso de esquemas rígidos (hardcoded) genera un alto nivel de acoplamiento entre el código fuente y las fuentes de datos.
Cualquier cambio en la estructura del archivo origen (añadir, renombrar o modificar tipos de datos)
requiere una intervención directa en el código, lo que limita la escalabilidad y agilidad para el onboarding
de nuevas fuentes de datos o variantes de los archivos actuales.

### Propuesta alternativa
Evolucionar hacia un modelo de **Ingesta Metadata-Driven**:

1. **Schema Registry:** Externalizar las definiciones de esquema en archivos de configuración (JSON/YAML)
    o un servicio de catálogo centralizado. El pipeline leería el esquema dinámicamente en tiempo de ejecución.
2. **Dynamic Schema Mapping:** Implementar un factory pattern que transforme los metadatos almacenados al `StructType`
    de Spark antes de la lectura.
3. **Schema Evolution:** Aprovechar las capacidades de Delta Lake (`mergeSchema`) para propagar cambios en la
    estructura de forma automática y controlada, en lugar de bloquear la ejecución por discrepancias menores.

**Trade-offs:**
- (+) **Escalabilidad:** Permite agregar nuevas fuentes sin necesidad de redeployar o modificar el código core del pipeline.
- (+) **Agilidad:** Facilita el mantenimiento al separar la lógica de negocio de la estructura física de los datos.
- (-) **Complejidad inicial:** Requiere implementar una capa de gestión de metadatos más robusta y un proceso de
    validación para asegurar que los cambios de esquema sean compatibles.

**Resolución en esta implementación:** Se ha mantenido el esquema explícito como una medida de seguridad estricta
para garantizar la integridad durante el MVP. Se documenta esta observación como la hoja de ruta necesaria para
escalar a una arquitectura *Metadata-Driven* que permita la interoperabilidad con múltiples fuentes de datos
de forma sostenible.

---

## OBS-06 — Aislamiento por schema es correcto para onboarding ágil, pero insuficiente como garantía de seguridad fuerte entre tenants

### Decisión de la arquitectura provista

La plataforma adopta **aislamiento por schema dentro de un único catálogo** por ambiente
(`saas_dev`, `saas_qa`, `saas_main`). Todos los tenants (SV, HN, GT, etc.) conviven en el mismo
catálogo Unity Catalog, separados únicamente por schema: `saas_dev.bronze_sv`, `saas_dev.bronze_hn`, etc.

### Punto de desacuerdo

El aislamiento por schema es un límite *lógico*, no un límite de *seguridad dura*. Esto implica:

1. **Blast radius de un grant incorrecto:** Un `GRANT SELECT ON SCHEMA saas_dev.silver_sv TO user@corp.com`
   accidental expone datos de un tenant completo. En un modelo catálogo-por-tenant, ese mismo
   error no cruza fronteras.
2. **Data residency:** Si CBC (por ejemplo) está sujeta a regulaciones locales que prohíben
   que sus datos convivan en el mismo catálogo que otra unidad de negocio extranjera, este
   modelo requiere excepciones o workarounds costosos.
3. **Límite de schemas por catálogo en Unity Catalog:** Aunque actualmente es generoso (miles),
   en una corporación multinacional con decenas de países y múltiples ambientes se convierte
   en un riesgo de planificación de capacidad.

### Propuesta alternativa

Un modelo **catálogo-por-unidad-de-negocio** (CBC, Beliv, BIA) con schemas por ambiente y capa
dentro de cada catálogo:

```
cbc_prod.bronze.sv_deliveries
cbc_prod.silver.sv_fact_deliveries
beliv_prod.bronze.gt_deliveries
```

**Trade-offs de esta propuesta:**
- (+) Límite de seguridad duro entre unidades de negocio; governance a nivel de catálogo por equipo dueño.
- (+) Facilita exigencias de data residency si cada catálogo puede mapearse a una región de ADLS distinta.
- (-) El análisis cross-tenant (actualmente posible con vistas en Gold sobre schemas del mismo catálogo)
  requeriría Delta Sharing o queries federadas entre catálogos.
- (-) El onboarding de un nuevo país dentro de la misma unidad de negocio sigue siendo un schema,
  pero el onboarding de una unidad de negocio nueva requiere un nuevo catálogo y su Terraform asociado.

**Resolución en esta implementación:** Se implementó la arquitectura provista (schema-per-tenant,
catálogo único). La simulación local con paths refleja la misma estructura lógica.

---

## OBS-07 — La columna `tenant_id` como partición en Bronze es redundante en el modelo de paths y genera fricción en la estrategia de idempotencia

### Decisión de la arquitectura provista

La sección 5.4 define que Bronze se particiona por `fecha_proceso` **y** `tenant_id`.
La sección 5.5 define que la idempotencia se logra con `replaceWhere` sobre esa combinación.
Al mismo tiempo, la estructura de paths ya segrega por tenant: `data/bronze/<tenant>/<table>/`.

### Punto de desacuerdo

Hay una **doble representación del tenant** en la capa Bronze local:

```
# El tenant ya está en el path:
data/bronze/sv/deliveries/fecha_proceso=20250314/

# Y también como columna de partición Delta:
fecha_proceso=20250314/tenant_id=sv/
```

Esto produce dos efectos no deseados:

1. **Archivos pequeños:** La partición Delta incluye `tenant_id`, añadiendo un nivel de
   subdirectorio sin valor en el contexto de paths aislados por tenant. En Databricks con
   Auto Loader y múltiples tenants en paralelo esto puede agravar el problema de small files.
2. **`replaceWhere` con predicado compuesto:** El overwrite idempotente requiere
   `replaceWhere("fecha_proceso = '20250314' AND tenant_id = 'sv'")`, en lugar del más simple
   `replaceWhere("fecha_proceso = '20250314'")`, sin ganancia real porque el archivo ya está
   en el directorio del tenant.

### Propuesta alternativa

- En **local/paths**: partición solo por `fecha_proceso`. El tenant queda implícito en el path.
  La columna `_tenant_id` se conserva como columna técnica (no de partición).
- En **Unity Catalog productivo**: el schema ya aísla el tenant; la partición por `tenant_id`
  tampoco agregaría valor allí.
- Si se desea partición compuesta, el orden correcto sería `(fecha_proceso, tenant_id)` para
  que el partition pruning por fecha (el predicado más común) sea eficiente.

**Trade-offs:**
- (+) Estructura Delta más limpia, `replaceWhere` más simple, mejor para Auto Loader incremental.
- (-) En un escenario donde Bronze fuera una tabla única (sin separación por path/schema),
  la partición por `tenant_id` sería indispensable para el aislamiento de escritura.

**Resolución en esta implementación:** Se implementó la partición compuesta tal como la define
la arquitectura. La columna `_tenant_id` existe tanto como partición como columna técnica.

---

## OBS-08 — La tabla de cuarentena no tiene ciclo de vida definido; sin un pipeline de reprocessing es un dead-end operacional

### Decisión de la arquitectura provista

La sección 5.6 define que los registros con anomalías críticas se escriben en una tabla paralela
de cuarentena con una columna `_quarantine_reason`. La sección 5.9 define `quality_logs` como
tabla de log de validaciones.

### Punto de desacuerdo

La arquitectura define correctamente *qué va a cuarentena* y *cómo se registra*, pero **no define**:

1. **Cómo se resuelven los registros en cuarentena.** Sin un pipeline de reprocessing, la cuarentena
   es un dead-end: los registros se acumulan y nunca retornan a la capa Silver.
2. **Correlación entre cuarentena y quality_logs.** `quality_logs` registra conteos agregados por
   check, pero no hay una clave que vincule un registro individual en cuarentena con el `_run_id`
   o `_batch_id` del log que lo reportó.
3. **Estado del registro.** No hay columna de estado (`pending` / `resolved` / `discarded`)
   que distinga registros nuevos de los ya revisados.

### Propuesta alternativa (Horizonte 2)

Extender el esquema de cuarentena con columnas de lifecycle:

```python
_quarantine_id:     string   # UUID único (FK implícita hacia quality_logs)
_quarantine_status: string   # 'pending' | 'resolved' | 'discarded'
_resolved_at:       timestamp
_resolved_by:       string   # usuario o proceso que resolvió
```

Y un pipeline de reprocessing parametrizable por tenant y reason:

```bash
python -m saas_pipeline.cli resolve-quarantine \
  --tenant sv \
  --reason "material_not_in_catalog" \
  --after-date 2025-03-01
```

Este pipeline leería la cuarentena filtrada, re-evaluaría las reglas (por ejemplo, tras actualizar
el catálogo de materiales) y reinjectaría los registros resueltos en Bronze para que fluyan
naturalmente por el pipeline.

**Trade-offs:**
- (+) La cuarentena se convierte en un área de staging auditada, no un cementerio de datos.
- (+) Permite SLAs de resolución ("todos los registros en cuarentena deben resolverse en N días").
- (-) Añade complejidad; requiere idempotencia adicional en el reprocessing.
- (-) Estado mutable en cuarentena puede generar confusión si no se controla con Delta versioning.

**Resolución en esta implementación:** Se implementó la cuarentena con el esquema mínimo definido
en la arquitectura. Se añadió `_batch_id` como único vínculo de correlación con `quality_logs`.
Las columnas de lifecycle propuestas no se implementaron para no modificar la arquitectura unilateralmente.

---

## OBS-09 — La validación de `fecha_proceso` en Bronze no detecta fechas de calendario inválidas; el registro queda mal clasificado en cuarentena Silver

### Decisión de la arquitectura provista

La sección 5.6 define que las filas con `fecha_proceso` nula o con formato inválido se aíslan en
`bronze_quarantine`. La validación implementada en `bronze.py` usa el patrón regex `^\d{8}$`,
que verifica exclusivamente que el valor tenga exactamente 8 dígitos numéricos.

### Punto de desacuerdo

El patrón `^\d{8}$` es **necesario pero no suficiente**. Una fecha como `20250230`
(30 de febrero de 2025) supera la validación de formato, llega a Bronze como dato válido y genera
una partición `fecha_proceso=20250230/` en el Delta table. El problema se manifiesta en Silver:

1. **Conversión silenciosa a `null`:** `F.to_date(F.col("fecha_proceso"), "yyyyMMdd")` retorna
   `null` para cualquier fecha de calendario inexistente. El registro no genera excepción;
   simplemente pierde su valor de fecha en `_fecha_dt`.
2. **Clasificación incorrecta en cuarentena:** Como `_fecha_dt` es `null`, el join temporal con
   `dim_materials` no produce match para ningún material. El registro termina en `silver_quarantine`
   con `_quarantine_reason = 'material_not_in_catalog'`, cuando la causa real es una fecha inválida
   (`invalid_calendar_date`). Esto contamina las métricas de calidad: los conteos de
   `material_not_in_catalog` incluyen registros que en realidad tienen un problema de fecha.
3. **Partición huérfana en Bronze:** La partición `fecha_proceso=20250230/` existe en el Delta
   table pero nunca generará datos en Silver, acumulándose silenciosamente entre runs.

### Propuesta alternativa

Extender la validación en `bronze.py:_split_by_fecha_validity` para incluir una verificación de
fecha de calendario real, además del formato:

```python
from pyspark.sql import functions as F

valid_cond = (
    F.col("fecha_proceso").isNotNull()
    & F.col("fecha_proceso").rlike(r"^\d{8}$")
    & F.to_date(F.col("fecha_proceso"), "yyyyMMdd").isNotNull()  # descarta fechas imposibles
)
```

Con este cambio, `20250230` sería detectado en Bronze y enviado a `bronze_quarantine` con
`_quarantine_reason = 'invalid_calendar_date'`, en lugar de propagarse a Silver con una razón
de cuarentena incorrecta.

**Trade-offs:**
- (+) La razón de cuarentena refleja con exactitud la anomalía del dato; las métricas de calidad
  quedan limpias y accionables.
- (+) Elimina particiones huérfanas en Bronze que de otro modo se acumulan indefinidamente.
- (+) El costo es mínimo: `F.to_date()` ya se ejecuta en Silver; moverlo a Bronze no añade
  complejidad significativa al pipeline.
- (-) Requiere que `fecha_proceso` sea evaluable por `to_date` en Bronze, lo que implica una
  dependencia implícita en el formato (`yyyyMMdd`) conocido desde esa capa. Si el formato
  cambiara, habría que actualizar la validación en dos lugares (Bronze y Silver).

**Resolución en esta implementación:** Se mantuvo la validación con `^\d{8}$` tal como la define
la sección 5.6 de la arquitectura. El comportamiento actual (fecha inválida → cuarentena Silver
con razón `material_not_in_catalog`) se identificó durante la ejecución local al observar la
partición `fecha_proceso=20250230/` en `data/bronze/hn/deliveries/`. Se documenta aquí para
corrección en la siguiente iteración y discusión en la sustentación.

---
