# Trabajo 2: pipeline batch y streaming en AWS
 
**Curso:** Almacenamiento y Procesamiento de Grandes Datos, 2026 (Maestría en Ciencias de los Datos y Analítica, Universidad EAFIT)
**Profesor:** Edwin Montoya
**Integrante:** Lorena Goez Ruiz
**Video:** https://youtu.be/XZgipkYtzxc
**Informe completo (reproducibilidad):** [INFORME_final.md](INFORME_final.md)
 
## Estado
 
| Parte | Estado |
|---|---|
| **A. Batch** (Glue, Step Functions, EventBridge, Athena) | **Parcial.** Datos aumentados a 5.025.000 filas y cargados en S3. Los scripts de Glue están escritos, pero **no se ejecutaron en AWS**: la cuenta tiene Glue bloqueado (`AccessDeniedException: Account ... is denied access`, con verificación de cuenta en progreso). Caso abierto en AWS Support. |
| **B. Streaming** (Kinesis, Lambda, DynamoDB, Firehose, S3) | **Completa y ejecutada en AWS.** |
 
Todos los recursos se crearon manualmente en la consola de AWS (us-east-1), sin infraestructura como código. CloudFormation no tiene ninguna pila.
 
## Resultado de la Parte B (corrida final, 330 s a 10 eventos/s)
 
| Publicados | Únicos | Repetidos | Inválidos | Cuadre |
|---:|---:|---:|---:|---|
| 3300 | 3035 | 115 | 150 | 3035 + 115 + 150 = 3300 |
 
Pregunta del proyecto: la categoría con mayor valor promedio por evento es **Electronics (256,93)**.
 
## Estructura del repositorio
 
```
.
├── README.md
├── INFORME_final.md
├── parte_a_batch/
│   ├── 01_aumentar_datos.py
│   ├── glue_job1_limpieza_join.py
│   ├── glue_job2_agg_categoria.py
│   └── glue_job3_agg_pago.py
├── parte_b_streaming/
│   ├── lambda_ventanas.py
│   ├── productor.py
│   ├── verificar_conteos.py
│   └── limpiar_tablas.py
└── evidencias/
```
 
## Cómo reproducir la Parte B
 
Requisitos: cuenta de AWS con plan de pago (Kinesis no está en el plan gratuito), Python 3.9+, `boto3` y AWS CLI configurada. No hay credenciales en este repositorio.
 
1. Crear las 4 tablas de DynamoDB (`t2_ventanas`, `t2_eventos_vistos`, `t2_eventos_invalidos`, `t2_eventos_repetidos`) y el stream `t2-eventos-stream` (aprovisionado, 1 fragmento).
2. Crear la Lambda con `lambda_ventanas.py` y su desencadenador de Kinesis; luego el Firehose hacia S3.
3. Ejecutar:
```
   python productor.py
   python verificar_conteos.py
```
 
Los pasos detallados con todos los parámetros están en el [informe](INFORME_final.md).
