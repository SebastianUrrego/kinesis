# Taller Kinesis: streaming con Amazon Kinesis (Big Data e Ingeniería de Datos)

Fuente: `taller_aws_kinesis_aws_academy.md`, en esta carpeta. Es el enunciado oficial: si algo aquí lo contradice, gana el enunciado. Visión general en `README.md`.

## Objetivo

Construir y documentar un pipeline de streaming en AWS Academy Learner Lab:

```
productor.py ─► Kinesis Data Stream ─┬─► Lambda procesar-sensores (alertas > 8 °C → CloudWatch Logs)
                                     └─► Firehose ─► S3 (particionado por fecha) ─► Athena (opcional)
```

Orden de trabajo: Lab 0 (preparación) → 1 (stream) → 2 (productor) → 3 (consumidor) → 4 (Lambda) → 5 (Firehose/S3) → 6 (limpieza) → reto final e informe.

## Reglas que no se pueden romper

- Región **`us-east-1`** siempre (`region_name="us-east-1"` explícito en `boto3`).
- **No crear roles IAM.** Lambda, Firehose y EC2 usan `LabRole` o `LabInstanceProfile`. Nada de código ni de instrucciones que llamen a `iam create-role` o `attach-role-policy`.
- No usar Managed Service for Apache Flink, Kinesis Data Analytics for SQL (discontinuado) ni Kinesis Video Streams.
- Nombres fijos: stream `taller-kinesis-sensores`, Lambda `procesar-sensores`, bucket `taller-kinesis-<iniciales>-<número>`. Todo recurso nuevo lleva el prefijo `taller-kinesis-`.
- Modo **provisionado** (1 shard en los labs, 3 en el reto). No usar on-demand salvo para compararlo en el informe.
- Formato del evento (no cambiarlo): JSON con `contenedor_id` (`CT-001`…`CT-008`), `temperatura` (°C, float), `humedad` (%, float) y `timestamp` (ISO 8601 UTC). Una línea por registro, terminada en `\n`, para que Firehose y Athena puedan separarlos.
- `PartitionKey = contenedor_id` por defecto. La partition key constante solo se usa como experimento del Lab 2 o del reto.
- Umbral de alerta: `UMBRAL = 8.0` °C.
- Los consumidores deben ser **idempotentes**, porque la entrega es *al menos una vez*.
- Todo script incluye o documenta su limpieza. **El stream de Kinesis consume crédito mientras exista**, aunque termine la sesión del lab.
- No subir credenciales (`aws_access_key_id`, `aws_session_token`, `vockey.pem`, `~/.aws/credentials`) al repositorio.
- No hacer `git push` sin preguntar al usuario.
- Responder y documentar en **español**.

## Entorno y trampas conocidas

- Sesión del Learner Lab: unas 4 h. Al terminar, las credenciales caducan (`ExpiredToken`). Desde el portátil hay que copiar credenciales nuevas desde *AWS Details*. Es preferible ejecutar desde CloudShell.
- **CRLF:** esta máquina es Windows. Los scripts `.sh` y `.py` que vayan a Linux (CloudShell o Lambda) deben tener finales LF. No pegar scripts en formularios de la consola: subirlos por CloudShell o por la CLI. Si algo falla sin explicación, revisar primero `cat -A archivo | grep '\^M'`.
- Firehose con `LabRole` **no está confirmado** en el Learner Lab. Si falla, usar el **Plan B**: una Lambda que lea del stream y escriba objetos JSON en S3 con `boto3`, con claves `sensores/anio=YYYY/mes=MM/dia=DD/<shard>-<primerSeq>-<últimoSeq>.json`. Así la tabla de Athena funciona igual, y un reintento del mismo lote sobrescribe el objeto en vez de duplicarlo.
- Lambda con trigger de Kinesis: el `data` llega en **base64**. Un registro puede traer varias líneas JSON. Devolver `{"batchItemFailures": [...]}` (respuesta por lotes parcial).
- Límites por shard: escritura de 1 MB/s o 1000 registros/s, lectura de 2 MB/s compartida. Ante `ProvisionedThroughputExceededException`, reintentar con *backoff* exponencial solo los registros fallidos de `put_records` (los índices donde aparece `ErrorCode`).

## Estructura

```
src/productor.py              Lab 2: put_records en lotes de 10 (200 eventos), reintenta solo los fallidos; --key-constante
src/consumidor.py             Lab 3: lee todos los shards; --tipo TRIM_HORIZON|LATEST|AT_TIMESTAMP; --contar (reto)
lambda/procesar_sensores.py   Lab 4: alertas; devuelve batchItemFailures
lambda/plan_b_s3_writer.py    Plan B del Lab 5 (variable de entorno BUCKET)
lambda/contar_alertas.py      Reto: resumen por lote → s3://BUCKET/resumen-alertas/
sql/athena.sql                Tablas sensores y resumen_alertas (JsonSerDe) y consultas
scripts/crear_recursos.sh     <bucket> [firehose|planb]; SHARDS=3 RETO=1 para el reto. Idempotente
scripts/limpieza.sh           <bucket>: borra triggers, Lambdas, Firehose, stream y bucket, y verifica
src/productor_param.py        Ejercicios adicionales 1: --tasa --duracion --claves --sesgo, lotes de 500
src/consumidor_shard.py       Ej. 2, 3 y 5: un shard por proceso; --pausa, --seguir-hijos, --verificar
src/contador_por_shard.py     Ej. 4: registros por shard (hot shard), --minutos, --csv
src/consumidor_efo.py         Ej. 6: enhanced fan-out (SubscribeToShard); elimina el consumidor al salir
src/consumidor_leases.py      Reto de los ejercicios: leases en DynamoDB (tabla leases), checkpoint, reparto
tests/test_local.py           Pruebas sin AWS: python -m unittest discover -s tests -v
docs/respuestas.md            Preguntas de los labs y las 7 preguntas de repaso
docs/informe.md               Informe: diagrama, decisiones y costos (las tablas de resultados las rellena el usuario)
docs/evidencias/              Capturas (métricas, logs, objetos en S3, limpieza)
docs/ejercicios.md            Ejercicios adicionales: comandos, tablas de resultados y respuestas
docs/ejercicios_adicionales_enunciado.pdf   Enunciado de los ejercicios adicionales
```

Los ejercicios adicionales usan los nombres de su enunciado: streams `ej-compite` (1 shard), `ej-reparto` (3) y `ej-reshard` (2 → 4), y la tabla DynamoDB `leases`. Allí las claves van de `CT-001` a `CT-0NN` según `--claves`. `limpieza.sh` también los borra.

Nombres de los recursos creados por los scripts: Firehose `taller-kinesis-firehose`, Lambdas `procesar-sensores`, `taller-kinesis-planb-s3` y `taller-kinesis-contar-alertas`. Si se agrega un recurso, también hay que añadirlo a `limpieza.sh`.

Mantener el código corto y didáctico, como en el enunciado. Sin frameworks, sin IaC (CloudFormation o CDK) salvo que el usuario lo pida, y sin dependencias fuera de `boto3` y la biblioteca estándar.

## Reto final (rúbrica de 100 pts)

| Criterio | Pts | Qué hay que producir |
|---|---|---|
| Pipeline funcional | 30 | Productor → stream → Lambda, con logs de CloudWatch como evidencia |
| Persistencia en S3 | 20 | Objetos en S3 vía Firehose o Plan B |
| Partition key y shards | 20 | Stream de 3 shards. Conteo de registros por shard con `contenedor_id` frente a una key constante (`IncomingRecords` por shard o un consumidor que cuente por `ShardId`) |
| Informe y diagrama | 20 | `docs/informe.md`: arquitectura, provisionado frente a on-demand, buffer de Firehose y estimación de costo |
| Limpieza evidenciada | 10 | Salida de `scripts/limpieza.sh` y `aws kinesis list-streams` vacío |

Dimensionamiento: shards = max(⌈registros/s ÷ 1000⌉, ⌈MB/s ÷ 1⌉), más un margen. Ejemplo del enunciado: 5000 sensores × 1 KB/s → 5 shards mínimo, 6 o 7 recomendados.

## Al terminar cualquier tarea

- Recordar al usuario que ejecute la limpieza y pulse **End Lab** si dejó recursos creados.
- Si se escribieron scripts para Linux, verificar que no tengan `\r`.
