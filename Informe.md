# Informe Trabajo 2
 
**Curso:** Almacenamiento y Procesamiento de Grandes Datos
**Profesor:** Edwin Montoya
**Integrantes:** Lorena Goez Ruiz
**Video (YouTube):** [enlace]
**Región de AWS utilizada para el proyecto:** us-east-1 (N. Virginia)
 
> **Estado de la entrega (IMPORTANTE).**
> - La parte B del trabajo fue realizada en su totalidad y ejecutada en AWS sin ningún problema. Sin embargo la parte A, se completó de manera parcial: se hizo el aumento de datos a más de 5.000.000 de registros y su carga en S3. Se intentó hhacer un script inicial para los jobs en Glue, pero NO se ejecutaron en AWS, porque al crear un job, la consola devuelve `AccessDeniedException: Account <ID> is denied access`, incluso con el usuario raíz y después de mejorar el plan de la cuenta. CloudShell muestra además: *"Your account verification is in progress. This may take up to two days for new accounts"*. Se abrió un caso con AWS Support y se informó al profesor. Por eso la máquina de estados, el horario y las consultas de las agrupaciones no se ejecutaron.
> - Todos los recursos se crearon manualmente en la consola de AWS, sin infraestructura como código. CloudFormation no tiene ninguna pila (evidencia en el video).
 
---
 
## 1. Resumen de la solución
 
| Parte | Servicios | Estado |
|---|---|---|
| A. Batch | S3, Glue (PySpark), Step Functions, EventBridge Scheduler, Athena | Datos en S3 listos. Glue/Step Functions/Scheduler **no ejecutados** (cuenta bloqueada) |
| B. Streaming | Kinesis Data Streams, Lambda, DynamoDB, Firehose, S3 | **Completa** |
 
Conjunto de datos: *Customer Behavior & Churn Prediction Dataset V2* (Kaggle, sintético). Se usa la tabla `orders` (17 columnas) y como segundo conjunto `customers`, que se une por `customer_id`.
 
---
 
## 2. Parte A: pipeline batch
 
### 2.1 Datos de partida y aumento (ejecutado)
 
`orders.csv` tiene 1.000.000 de filas. El enunciado pide al menos 5.000.000, por lo que se **aumenta** con `01_aumentar_datos.py`:
 
1. Se replica el conjunto 5 veces. En cada copia k ≥ 1 se agrega el sufijo `-k` al `order_id` y se multiplican `unit_price` y `order_amount` por un mismo factor aleatorio entre 0,9 y 1,1 (semilla fija 42). `customer_id` no cambia, para que el join con `customers` siga funcionando.
2. Se inyecta ruido controlado y declarado para que la limpieza tenga qué corregir: 0,5 % de `customer_id` vacío, 0,3 % de fechas inválidas, 0,3 % de montos negativos y 0,5 % de filas duplicadas.
```
python 01_aumentar_datos.py orders.csv
```
 
Resultado: carpeta `orders_aug/` con `orders_part_1.csv` a `orders_part_5.csv` (**5.025.000 filas** en total).
 
### 2.2 Carga en S3 (ejecutado)
 
1. Crear el bucket `trabajo2-lorena-` (uso general, ACL deshabilitadas, bloqueo de acceso público activado).
2. Subir los 5 CSV a `raw/orders/` y `customers.csv` a `raw/customers/`.
### 2.3 Rol de IAM para Glue
 
IAM → Roles → Crear rol → servicio **Glue** → políticas `AWSGlueServiceRole` y `AmazonS3FullAccess` → nombre `rol-glue-t2`.
 
### 2.4 Trabajos de Glue (scripts escritos; **no ejecutados**)
 
Glue → ETL jobs → Script editor → motor **Spark** → Start fresh. Pegar el script, cambiar `BUCKET` por el nombre del bucket y, en *Job details*: rol `rol-glue-t2`, Glue 4.0 o superior, worker **G.1X**, **2 workers**, timeout 30 min, reintentos 0.
 
| Job (nombre exacto) | Script | Qué hace | Salida |
|---|---|---|---|
| `t2-job1-limpieza-join` | `glue_job1_limpieza_join.py` | Lee CSV de orders y customers, limpia, une por `customer_id` | `curated/orders_enriched/` (Parquet, particionado por `order_year`) |
 
**Limpieza (job 1):** se descartan filas con `order_id` o `customer_id` vacío, fecha que no se puede convertir a `yyyy-MM-dd`, monto nulo o negativo, cantidad nula o menor o igual a 0, descuento fuera de 0 a 100 y `order_id` duplicado. Cada etapa imprime sus conteos (`[LECTURA]`, `[LIMPIEZA]`, `[JOIN]`, `[ESCRITURA]`).
 
**Join:** inner join por `customer_id`. Las columnas de `customers` se renombran con el prefijo `cust_` para evitar choques de nombres.
 
**Partición por `order_year`:** las consultas de analítica agrupan o filtran por tiempo, hay pocas particiones (un directorio por año), no se generan archivos diminutos y Spark o Athena se saltan directorios completos. *(Si todas las fechas cayeran en un solo año, se cambia por `order_month`.)*
 
**Preguntas de analítica:**
- Rama 1 (`agg_categoria_mensual`): ¿qué categoría genera más ingreso y con qué tasa de devolución?
- Rama 2 (`agg_metodo_pago`): ¿qué método de pago se asocia con más devoluciones, peor calificación y más días de entrega?
### 2.5 Resultados de la Parte A
 
| Elemento | Resultado |
|---|---|
| Registros de partida | 5.025.000 en S3 (`raw/orders/`) |
| Ejecución de Glue, máquina de estados y horario | **No realizada**: cuenta bloqueada  |
| Agrupaciones y consultas | **No disponibles** |
 
---
 
## 3. Parte B: streaming 
 
### 3.1 Arquitectura
 
```
productor.py ──► Kinesis Data Streams (t2-eventos-stream, 1 fragmento)
                        │
                        ├──► Lambda t2-ventanas ──► DynamoDB: t2_ventanas / t2_eventos_vistos
                        │                                      t2_eventos_invalidos / t2_eventos_repetidos
                        └──► Firehose t2-raw-a-s3 ──► S3 raw-events/  (eventos crudos)
```
 
**Evento:**
```json
{"event_id":"uuid","categoria":"Electronics","valor":123.45,"event_ts":1760000000123,"usuario":"U00012"}
```
 
### 3.2 Orden de creación (importante)
 
Crear en este orden, porque la Lambda y Firehose leen desde *Latest* y no recuperan eventos anteriores: **tablas → stream → Lambda con su desencadenador → Firehose → productor**.
 
### 3.3 Tablas de DynamoDB (capacidad bajo demanda)
 
| Tabla | Clave de partición | Clave de ordenación | Contenido |
|---|---|---|---|
| `t2_ventanas` | `categoria` (S) | `ventana` (S, `AAAA-MM-DDTHH:MM` UTC) | `conteo`, `suma_valor`, `promedio` |
| `t2_eventos_vistos` | `event_id` (S) | — | id de cada evento ya contado |
| `t2_eventos_invalidos` | `registro_id` (S) | — | motivo y evento original |
| `t2_eventos_repetidos` | `registro_id` (S) | — | `event_id` repetido |
 
`registro_id` es el *sequence number* de Kinesis, único por registro.
 
### 3.4 Stream de Kinesis
 
Kinesis Data Streams → Crear → nombre `t2-eventos-stream`, modo **Aprovisionado**, **1 fragmento**, estrategia de distribución **Definida por el usuario**. Se usa 1 fragmento para que la Lambda procese los lotes uno a uno y no haya transacciones concurrentes sobre la misma ventana.
 
### 3.5 Lambda `t2-ventanas`
 
1. Crear la función (Python 3.12 o superior) y pegar `lambda_ventanas.py`. Pulsar **Deploy**.
2. Configuración general: memoria 256 MB, timeout 60 s.
3. Al rol de ejecución agregar `AWSLambdaKinesisExecutionRole` y `AmazonDynamoDBFullAccess`.
4. Desencadenador de Kinesis: stream `t2-eventos-stream`, tamaño de lote **100**, posición inicial **Latest**, ventana de lote **2 s**, lotes simultáneos por fragmento **1**, reintentos **3**, **Report batch item failures** activado.
**Lógica (por cada registro):**
 
| Caso | Regla | Acción |
|---|---|---|
| Inválido | JSON inválido, falta `event_id`/`categoria`/`valor`/`event_ts`, `valor` no numérico o fuera de [1, 1000], `event_ts` inválido | Se guarda en `t2_eventos_invalidos` con el motivo |
| Repetido | `event_id` ya contado | Se guarda en `t2_eventos_repetidos`, **no se cuenta** |
| Único | Evento nuevo y válido | Suma 1 al conteo y el valor a la suma de su ventana (categoría + minuto de `event_ts`) |
 
**Decisiones de diseño:**
- **Ventana de 1 minuto por tiempo del evento** (`event_ts`, UTC), por categoría. Se almacenan `conteo` y `suma_valor` con `ADD` atómico y después se recalcula `promedio` leyendo ambos valores con lectura consistente.
- **Sin doble conteo:** "marcar el evento como visto" (`t2_eventos_vistos`, con condición `attribute_not_exists`) y "sumar a la ventana" se hacen en **una sola transacción** (`transact_write_items`). Si Kinesis reintenta un lote, la Lambda distingue un reintento del mismo registro (mismo `registro_id`) de un repetido verdadero (otro `registro_id`).
- **Por qué Lambda y no Flink:** el enunciado permite Lambda. Flink exige empaquetar una aplicación y era demasiado riesgoso para el tiempo disponible.
### 3.6 Segundo consumidor: Firehose hacia S3
 
Amazon Data Firehose → origen *Kinesis Data Streams* (`t2-eventos-stream`) → destino S3 (`trabajo2-lorena-2026`), prefijo `raw-events/`, búfer de 60 s, sin compresión, nombre `t2-raw-a-s3`. Cada registro termina en salto de línea (lo agrega el productor), así que los archivos son JSON por líneas. *Nota:* si Firehose falla con "unable to assume role" justo después de crear el rol automático, esperar 1 o 2 minutos y reintentar.
 
### 3.7 Ejecución
 
```
cd parte_b_streaming
python productor.py --segundos 20        # prueba corta
python verificar_conteos.py              # debe decir CUADRA
python limpiar_tablas.py                 # vacía las 4 tablas
# vaciar también raw-events/ en S3
python productor.py                      # corrida final: 330 s a 10 eventos/s
python verificar_conteos.py
```
 
El productor repite a propósito ~4 % de eventos ya enviados y envía ~4 % de inválidos (sin `categoria`, o `valor` en {-50, 0, 5000, 99999}). Hay que cambiar `CATEGORIAS` por categorías reales del proyecto. Esperar ~2 minutos tras terminar para que Firehose escriba el último bloque.
 
### 3.8 Resultados de la Parte B
 
 
| Concepto | Cantidad |
|---|---:|
| Duración del productor | 330,3 s |
| **Publicados** | 3300 |
| Únicos procesados | 3035 |
| Repetidos descartados | 115 |
| Inválidos descartados | 150 |
| Únicos + repetidos + inválidos | 3300 (**CUADRA**) |
 
**Motivos de descarte (inválidos):**
 
| Motivo | Cantidad |
|---|---:|
| `campo_faltante:categoria` | 71 |
| `valor_fuera_de_rango` | 79 |
 
**Pregunta del proyecto:** *¿qué categoría tiene el mayor valor promedio por evento?* Respuesta: Electronics, con promedio 256,93 (ver tabla de `verificar_conteos.py`).
 
**Consulta en DynamoDB (consola):** `t2_ventanas` → Explorar elementos → Consultar → `categoria` = Electronics → muestra `conteo` y `promedio` por minuto.
 
**Eventos crudos:** `s3://trabajo2-lorena-2026/raw-events/AAAA/MM/DD/HH/` con todos los eventos tal como llegaron.
 
---
 
## 4. Limitaciones y problemas conocidos
 
1. **Glue bloqueado en la cuenta (Parte A incompleta).** Error `AccessDeniedException: Account <ID> is denied access` al crear cualquier job, aun como usuario raíz y con el plan de pago. CloudShell indica verificación de cuenta en progreso. Caso abierto en AWS Support. Hasta que se habilite, no se pueden ejecutar los jobs, la máquina de estados, el horario ni las consultas de las agrupaciones.
2. **Horas en UTC.** Las ventanas se calculan en UTC; Colombia está 5 horas atrás.
3. **Lambda en lugar de Flink** para las ventanas (permitido por el enunciado). Con un solo fragmento el rendimiento es suficiente para 10 eventos por segundo; con varios fragmentos habría que revisar la concurrencia de las transacciones.
4. **Consulta opcional de `raw-events` en Athena:** puede dar 0 si Athena no lee las subcarpetas. La evidencia de los eventos crudos es la carpeta en S3.
---
 
## 5. Limpieza de recursos
 
Al terminar, borrar para evitar consumo de créditos: stream de Kinesis y Firehose (cobran por hora de existencia), Lambda y su desencadenador, jobs de Glue (si se crearon), máquina de estados, horario, tablas de DynamoDB y bucket. Eliminar las **claves de acceso** del usuario de IAM usado por el productor. Verificar que CloudFormation → Pilas siga en 0.
 
---
 
## 6. Declaración de uso de inteligencia artificial
 
Se usó **Claude (Anthropic)** como asistente durante este trabajo. Declaración explícita de qué fue generado por IA:
 
| Elemento | Generado por IA | Revisado por el equipo |
|---|---|---|
| `01_aumentar_datos.py` | Sí (código) | Sí |
| `glue_job1_limpieza_join.py`| Sí (código) | Sí |
| `lambda_ventanas.py` (lógica de ventanas, deduplicación transaccional) | Sí (código) | Sí |
| `productor.py`, `verificar_conteos.py`, `limpiar_tablas.py` | Sí (código) | Sí |
| Guías paso a paso| Sí (texto) | Sí |
| Redacción de informe | Sí (texto) | Sí, completado con resultados propios |
 
