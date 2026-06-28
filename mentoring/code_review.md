# Code Review — bad_code.py

**Revisado por:** Senior Data Engineer  
**Contexto:** Entrega hipotética de un ingeniero junior del equipo para procesar entregas
de producto por país usando PySpark.

---

## Observación 1 — Uso de pandas donde corresponde Spark nativo (impacto: crítico)

**Qué está mal:**

```python
df = pd.read_csv(file_path)       # <- Carga el CSV en memoria del driver
for i, row in df.iterrows():      # <- Itera fila por fila en Python puro
```

El código carga el CSV completo en el nodo driver con pandas y luego itera fila por fila.
Esto elimina completamente el beneficio de tener Spark disponible. Con 3,100 filas funciona;
con 3,100,000 filas el driver consume toda la memoria y el job muere.

**Por qué importa:**

La razón de usar PySpark es el procesamiento distribuido enfocado en paralelizar cargas de trabajo masivas.
Procesar los datos con Pandas en el driver desperdicia la infraestructura distribuida y limita
el rendimiento a la capacidad de un único componente.

**Cómo se corrige:**

```python
# Usar Spark nativo para leer y transformar
df = spark.read.option("header", "true").csv(file_path)
df_filtered = df.filter(F.col("pais") == country)
df_result = df_filtered.withColumn(
    "cantidad_st",
    F.when(F.col("unidad") == "CS", F.col("cantidad") * 20).otherwise(F.col("cantidad"))
)
```

---

## Observación 2 — Lógica de negocio y paths hardcoded; diseño no multi-tenant (impacto: crítico)

**Qué está mal:**

```python
def process(file_path, country):     # <- "country" como string arbitrario
    df = df[df["pais"] == country]   # <- Filtro WHERE pais = X
    sdf.write.parquet("/tmp/output/" + country)  # <- Path hardcoded
```

El parámetro `country` es un string libre sin validación. Los paths de salida se construyen
concatenando strings, lo que genera rutas distintas para `"GT"` vs `"gt"` o `"Gt"`. La lógica
de filtrado no es reutilizable ni configurable: si el campo cambia de `pais` a `tenant_id`,
hay que modificar la función.

**Por qué importa:**

En un pipeline multi-tenant real, los paths, las claves de partición y los identificadores
de tenant deben ser consistentes y centralizados. Rutas hardcoded en `/tmp/` no sobreviven
a un reinicio del cluster y no tienen idempotencia.

**Cómo se corrige:**

```python
# paths centralizados en configuración
output_path = f"{cfg.paths.bronze}/{tenant_code}/deliveries"
# normalización consistente
tenant_code = country.lower()
df = df.filter(F.col("_tenant_id") == tenant_code)
```

---

## Observación 3 — Ausencia de validaciones y manejo de errores; fallo silencioso (impacto: alto)

**Qué está mal:**

```python
result.append({
    ...
    "total": qty * row["precio"]   # <- ¿qué pasa si precio es None?
})
```

No hay validación de ningún campo. Si `precio` es `None`, el producto produce `None` y el
registro entra a la salida silenciosamente. Si `cantidad` es negativa o cero, también entra.
No hay logging, no hay cuarentena, no hay conteo de anomalías.

**Por qué importa:**

Los datos corruptos que llegan a producción silenciosamente producen métricas incorrectas que
pueden tardar días en detectarse. Un campo `precio = None` propagado a Gold produce `revenue = None`,
que luego aparece como 0 en dashboards.

**Cómo se corrige:**

```python
# Separar anomalías explícitamente antes de procesar
df_valid     = df.filter(F.col("precio").isNotNull() & (F.col("cantidad") > 0))
df_quarantine = df.filter(F.col("precio").isNull() | (F.col("cantidad") <= 0))
# registrar cuántos registros fueron descartados
logger.warning("Filas en cuarentena: %d", df_quarantine.count())
```

---

## Observación 4 — Escritura no idempotente en formato no versionado (impacto: alto)

**Qué está mal:**

```python
sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
```

La escritura usa Parquet (no Delta) con `mode("overwrite")` sobre la ruta raíz completa.
Esto borra **todo** el output anterior del país, incluyendo fechas distintas a la procesada.
Correr el pipeline dos veces con rangos de fechas diferentes resulta en pérdida de datos.

**Por qué importa:**

En producción, los pipelines se ejecutan diariamente sobre una partición de fecha. Si el
overwrite elimina fechas anteriores, el histórico se pierde. Además, Parquet no tiene
transacciones ACID: una escritura parcial puede dejar la tabla en un estado inconsistente.

**Cómo se corrige:**

```python
# Delta Lake con replaceWhere: solo sobreescribe la partición del rango procesado
(
    df_result.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("fecha_proceso")
    .option("replaceWhere", f"fecha_proceso = '{fecha}'")
    .save(output_path)
)
```

---

## Observación 5 — SparkSession global en módulo; estado compartido implícito (impacto: medio)

**Qué está mal:**

```python
spark = SparkSession.builder.getOrCreate()  # <- Nivel de módulo

def process(file_path, country):
    ...
    sdf = spark.createDataFrame(out)  # <- usa el global
```

La SparkSession se crea como variable global al importar el módulo. Esto dificulta los tests
(no se puede inyectar un mock), acopla la función a un estado externo invisible, y genera
comportamientos inesperados si el módulo se importa en un contexto donde ya existe una sesión.

**Cómo se corrige:**

```python
def process(spark: SparkSession, cfg: DictConfig, tenant: str) -> dict[str, int]:
    """spark se inyecta desde el llamador — testeable e independiente."""
    ...
```

---

## Cómo se lo explicaría al junior

El código demuestra que entendiste el **qué** de la tarea: filtrar por país, convertir unidades,
calcular el total, escribir el resultado. Eso eslo importante como punto de partida. Lo que te pido
que investigues por tu cuenta son las razones del **¿cómo?** en este tipo de proyectos:

1. **¿Por qué no iteramos fila a fila en Spark?** Busca "PySpark DataFrame API vs RDD" y el concepto
   de *lazy evaluation*. La clave está en que Spark ejecuta las transformaciones de forma distribuida:
   si iteramos con Python, todo pasa en el driver y perdemos eso completamente.

2. **¿Qué problema resuelve Delta Lake que Parquet no resuelve?** Busca "ACID transactions in data lakes"
   y el concepto de "exactly-once writes". La respuesta explica por qué las escrituras con `mode("overwrite")`
   sobre Parquet son peligrosas en producción.

3. **¿Por qué la configuración debe venir de fuera de la función?** Busca "Dependency Injection".
   Un pipeline hardcodeado con rutas y parámetros nunca puede
   moverse de local a producción sin modificar el código.

El feedback no va sobre lo que falta: va sobre construir el hábito de preguntarse "¿qué pasa cuando
esto escala a 10× los datos?" y "¿qué pasa si esto falla a la mitad?". Esas preguntas distinguen
el código que funciona del código que sobrevive en producción.
