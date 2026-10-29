# Taller: Streaming de datos con Amazon Kinesis en AWS Academy (Learner Lab)

**Curso:** Big Data e Ingeniería de Datos
**Duración sugerida:** 3 horas (45 min de teoría + 2 h 15 min de práctica)
**Región:** us-east-1 (N. Virginia) — el Learner Lab solo permite us-east-1 y us-west-2
**Entorno:** AWS Academy Learner Lab (usuario `voclabs`, rol `LabRole`)

---

## 0. Verificación de compatibilidad con AWS Academy

Antes de la clase, abra el Learner Lab y compruebe cada servicio. La lista oficial de servicios cambia por semestre y por tipo de laboratorio, así que **esta tabla es una guía, no una garantía**.

| Servicio | Uso en el taller | Estado | Cómo verificarlo |
|---|---|---|---|
| Amazon Kinesis Data Streams | Núcleo del taller | Aparece en el listado "Associate Services" del Learner Lab (documento *Lab 1.1 – Limitations on Associate Services*) | Crear un stream desde la consola |
| Amazon S3 | Destino de Firehose | Servicio base del Learner Lab | Crear un bucket |
| AWS Lambda | Consumidor serverless | Servicio base del Learner Lab | Crear una función con `LabRole` |
| Amazon CloudWatch | Logs y métricas | Servicio base del Learner Lab | Ver logs de la Lambda |
| Amazon Data Firehose (antes Kinesis Data Firehose) | Entrega a S3 | **No confirmado en las fuentes consultadas** | Intentar crear un delivery stream y elegir `LabRole` |
| AWS Cloud9 o CloudShell | Ejecutar Python/CLI | Cloud9 aparece en el listado; CloudShell debe probarse | Abrir cada uno desde la consola |
| Amazon Athena (opcional) | Consultar lo que llegó a S3 | Aparece en el listado | Abrir el editor de consultas |
| Managed Service for Apache Flink | **No se usa** | No confirmado; además es un servicio de pago por KPU | Excluido del taller |
| Kinesis Data Analytics for SQL | **No se usa** | AWS lo discontinuó el 27 de enero de 2026 | Excluido del taller |
| Kinesis Video Streams | **No se usa** | Aparece en el listado, pero está fuera del alcance (video) | Solo se menciona en teoría |

**Restricciones típicas del Learner Lab que afectan el taller**

- No se pueden crear roles IAM propios: se usa siempre `LabRole` (y el perfil `LabInstanceProfile` para EC2).
- Las credenciales temporales caducan al terminar la sesión del lab (aprox. 4 horas). Si ejecutan scripts desde su portátil, deben copiar de nuevo las credenciales desde *AWS Details*.
- Al finalizar la sesión, algunos recursos se detienen, pero **un stream de Kinesis sigue consumiendo crédito** mientras exista. Hay que eliminarlo al final.
- El crédito del lab es limitado (normalmente alrededor de 50 a 100 USD, según el curso). Este taller cuesta centavos si se limpia al terminar.

**Plan B si Firehose no está disponible:** reemplace el Laboratorio 5 por una Lambda que lea del stream y escriba objetos JSON en S3 con `boto3`. Cumple el mismo objetivo de aprendizaje (persistir el stream en un data lake).

---

## 1. Objetivos de aprendizaje

Al terminar, el estudiante podrá:

1. Diferenciar procesamiento por lotes (batch) y por flujo (streaming).
2. Explicar los componentes de Kinesis Data Streams: shard, partition key, sequence number, retención.
3. Producir y consumir registros con `boto3`.
4. Disparar una función Lambda desde un stream.
5. Entregar el stream a S3 con Firehose y consultarlo.
6. Dimensionar un stream y justificar cuándo usar Data Streams frente a Firehose.

---

## 2. Teoría (45 minutos)

### 2.1 Batch vs streaming

| | Batch | Streaming |
|---|---|---|
| Datos | Conjuntos acotados y almacenados | Flujo continuo, no acotado |
| Latencia | Minutos u horas | Milisegundos a segundos |
| Ejemplo | ETL nocturno con Spark | Detección de fraude, telemetría, clickstream |
| Herramientas en AWS | Glue, EMR, Athena | Kinesis, MSK, Flink |

Idea clave: en streaming, el dato se procesa **cerca de cuando ocurre**, y el sistema debe tolerar desorden, duplicados y picos de carga.

### 2.2 La familia Kinesis

- **Kinesis Data Streams (KDS):** registro distribuido y duradero. Usted escribe los consumidores. Permite que varios consumidores lean lo mismo y permite reprocesar (replay).
- **Amazon Data Firehose:** servicio de entrega administrado. Sin código de consumidor; envía a S3, Redshift, OpenSearch, Splunk o endpoints HTTP. Trabaja por **buffer** (tamaño o tiempo), por lo que es *casi* tiempo real, no tiempo real.
- **Managed Service for Apache Flink:** procesamiento con estado (ventanas, agregaciones). Fuera de este taller.
- **Kinesis Video Streams:** ingesta y almacenamiento de video. Fuera de este taller.

### 2.3 Anatomía de Kinesis Data Streams

```
Productores ──► [ Stream ]
                 ├─ Shard 1 ──► Consumidores
                 ├─ Shard 2 ──► Consumidores
                 └─ Shard N ──► Consumidores
```

- **Record (registro):** unidad de dato. Contiene `Data` (blob de hasta 1 MB), `PartitionKey` y un `SequenceNumber` asignado por Kinesis.
- **Shard:** unidad de capacidad y de orden. Por shard (modo provisionado): escritura hasta **1 MB/s o 1000 registros/s**; lectura hasta **2 MB/s** (consumidores compartidos).
- **Partition key:** se aplica una función hash para decidir a qué shard va el registro. Mismo key → mismo shard → **orden garantizado dentro de ese key**. Una mala elección de key causa *hot shards*.
- **Retención:** 24 horas por defecto, ampliable hasta 365 días.
- **Modos de capacidad:**
  - *Provisionado:* usted elige el número de shards (previsible, barato para cargas estables).
  - *On-demand:* AWS escala automáticamente (cómodo, pero más caro en volumen sostenido).
- **Productores:** API (`PutRecord`/`PutRecords`), KPL (Kinesis Producer Library), Kinesis Agent, SDK.
- **Consumidores:** Lambda, KCL (Kinesis Client Library), Firehose, API directa (`GetRecords`). *Enhanced fan-out* da 2 MB/s dedicados por consumidor.
- **Semántica de entrega:** *al menos una vez*. Los consumidores deben ser **idempotentes**.
- **Seguridad:** IAM, cifrado en reposo con KMS, TLS en tránsito.

### 2.4 Amazon Data Firehose

- Parámetros de **buffer**: tamaño (MB) e intervalo (segundos). Entrega cuando se cumple lo primero.
- **Transformación** opcional con Lambda (enriquecer, filtrar, convertir formato).
- Conversión a Parquet/ORC (con Glue Data Catalog) y particionado por prefijo (`año/mes/día/hora`).
- Fuente: Direct PUT o un Kinesis Data Stream.

### 2.5 ¿Cuándo usar cada uno?

| Necesidad | Elección |
|---|---|
| Varios consumidores independientes del mismo flujo | Data Streams |
| Reprocesar datos históricos del flujo | Data Streams |
| Solo cargar el flujo a S3/Redshift sin código | Firehose |
| Latencia de milisegundos | Data Streams |
| Latencia tolerable de 60 s o más | Firehose |

### 2.6 Dimensionamiento (ejercicio rápido)

Una flota de 5000 sensores envía 1 mensaje de 1 KB cada segundo.
- Entrada: 5000 registros/s y ≈ 5 MB/s.
- Shards por registros: 5000 / 1000 = 5. Shards por volumen: 5 / 1 = 5.
- Resultado: **mínimo 5 shards**, y conviene dejar margen (por ejemplo 6 a 7).

---

## 3. Práctica

**Escenario:** una empresa de logística monitorea la temperatura y humedad de contenedores refrigerados. Los sensores envían eventos JSON; se necesita (a) alertar si la temperatura supera un umbral y (b) guardar todo en un data lake en S3.

```
Productor (Python) ─► Kinesis Data Stream ─┬─► Lambda (alertas, CloudWatch Logs)
                                           └─► Firehose ─► S3 ─► (Athena)
```

### Laboratorio 0: Preparación (10 min)

1. Inicie el Learner Lab y abra la consola de AWS (botón verde).
2. Confirme la región **us-east-1**.
3. Abra **CloudShell** (o Cloud9). Verifique las credenciales:
   ```bash
   aws sts get-caller-identity
   ```
4. Instale/compruebe boto3:
   ```bash
   python3 -c "import boto3; print(boto3.__version__)"
   ```

### Laboratorio 1: Crear el stream (10 min)

Consola → Kinesis → Data Streams → *Create data stream*.

- Nombre: `taller-kinesis-sensores`
- Modo de capacidad: **Provisionado**, **1 shard**
- Crear.

Equivalente en CLI:
```bash
aws kinesis create-stream --stream-name taller-kinesis-sensores --shard-count 1
aws kinesis describe-stream-summary --stream-name taller-kinesis-sensores
```

**Pregunta:** ¿cuál es la capacidad máxima de escritura de este stream?

### Laboratorio 2: Productor con boto3 (20 min)

Archivo `productor.py`:

```python
import boto3, json, random, time
from datetime import datetime, timezone

STREAM = "taller-kinesis-sensores"
kinesis = boto3.client("kinesis", region_name="us-east-1")

CONTENEDORES = [f"CT-{i:03d}" for i in range(1, 9)]

def generar_evento():
    return {
        "contenedor_id": random.choice(CONTENEDORES),
        "temperatura": round(random.gauss(5, 3), 2),   # °C
        "humedad": round(random.uniform(40, 90), 1),   # %
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

if __name__ == "__main__":
    enviados = 0
    while enviados < 200:
        lote = [generar_evento() for _ in range(10)]
        respuesta = kinesis.put_records(
            StreamName=STREAM,
            Records=[
                {
                    "Data": (json.dumps(e) + "\n").encode("utf-8"),
                    "PartitionKey": e["contenedor_id"],
                }
                for e in lote
            ],
        )
        fallidos = respuesta["FailedRecordCount"]
        enviados += len(lote) - fallidos
        print(f"Enviados: {enviados} | Fallidos en este lote: {fallidos}")
        time.sleep(1)
```

Ejecute: `python3 productor.py`

**Actividades**
- Observe en la consola la pestaña *Monitoring* (métrica `IncomingRecords`).
- Cambie el `PartitionKey` por una constante. ¿Qué implica con varios shards?

### Laboratorio 3: Consumidor manual (15 min)

Archivo `consumidor.py`:

```python
import boto3, json, time

STREAM = "taller-kinesis-sensores"
kinesis = boto3.client("kinesis", region_name="us-east-1")

shard_id = kinesis.describe_stream(StreamName=STREAM)["StreamDescription"]["Shards"][0]["ShardId"]

iterador = kinesis.get_shard_iterator(
    StreamName=STREAM,
    ShardId=shard_id,
    ShardIteratorType="TRIM_HORIZON",   # desde el registro más antiguo
)["ShardIterator"]

while iterador:
    salida = kinesis.get_records(ShardIterator=iterador, Limit=50)
    for r in salida["Records"]:
        evento = json.loads(r["Data"].decode("utf-8"))
        print(r["SequenceNumber"][-6:], evento)
    iterador = salida["NextShardIterator"]
    if not salida["Records"]:
        time.sleep(2)
```

**Actividades**
- Pruebe `LATEST`, `TRIM_HORIZON` y `AT_TIMESTAMP`. Explique la diferencia.
- Ejecute dos consumidores a la vez. ¿Ambos reciben los mismos registros? ¿Qué pasa con el límite de 2 MB/s compartido?

### Laboratorio 4: Lambda como consumidor (25 min)

1. Consola → Lambda → *Create function* → *Author from scratch*.
   - Nombre: `procesar-sensores`, Runtime: Python 3.x
   - Permisos: *Use an existing role* → **LabRole**
2. Código:

```python
import base64, json

UMBRAL = 8.0  # °C

def lambda_handler(event, context):
    for registro in event["Records"]:
        payload = base64.b64decode(registro["kinesis"]["data"]).decode("utf-8")
        for linea in payload.strip().splitlines():
            e = json.loads(linea)
            if e["temperatura"] > UMBRAL:
                print(f"ALERTA {e['contenedor_id']}: {e['temperatura']} °C")
            else:
                print(f"OK {e['contenedor_id']}: {e['temperatura']} °C")
    return {"batchItemFailures": []}
```

3. *Add trigger* → Kinesis → stream `taller-kinesis-sensores`.
   - Batch size: 100, Batch window: 5 s, Starting position: *Latest*.
4. Ejecute de nuevo `productor.py`.
5. Revise los logs en **CloudWatch → Log groups → /aws/lambda/procesar-sensores**.

**Actividades**
- Cambie *Batch size* a 5 y *Batch window* a 0. Compare el número de invocaciones.
- Discuta: ¿por qué la Lambda debe ser idempotente?

### Laboratorio 5: Firehose a S3 (25 min)

1. Cree un bucket S3: `taller-kinesis-<sus-iniciales>-<número>` (nombre único global).
2. Consola → Amazon Data Firehose → *Create Firehose stream*.
   - Source: **Amazon Kinesis Data Streams** → `taller-kinesis-sensores`
   - Destination: **Amazon S3** → su bucket
   - Prefijo: `sensores/anio=!{timestamp:yyyy}/mes=!{timestamp:MM}/dia=!{timestamp:dd}/`
   - Buffer: **1 MB / 60 segundos**
   - Permisos IAM: *Choose existing IAM role* → **LabRole**
3. Ejecute `productor.py` un par de veces y espere más de 60 s.
4. Verifique los objetos en S3 y revise la métrica de entrega en Firehose.

**Si Firehose o `LabRole` no se acepta:** aplique el Plan B de la sección 0.

**Extensión opcional (Athena):** cree una tabla externa sobre `s3://<bucket>/sensores/` y consulte la temperatura promedio por contenedor.

```sql
CREATE EXTERNAL TABLE sensores (
  contenedor_id string,
  temperatura double,
  humedad double,
  `timestamp` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<su-bucket>/sensores/';

SELECT contenedor_id, avg(temperatura) AS temp_prom, count(*) AS n
FROM sensores
GROUP BY contenedor_id
ORDER BY temp_prom DESC;
```

### Laboratorio 6: Limpieza (5 min) — obligatorio

```bash
aws kinesis delete-stream --stream-name taller-kinesis-sensores --enforce-consumer-deletion
```
Elimine también: el delivery stream de Firehose, la función Lambda (y su trigger), y vacíe y elimine el bucket S3. Luego pulse **End Lab**.

---

## 4. Reto final (individual o por parejas)

Extienda el pipeline:
1. Use **3 shards** y compare el reparto de registros por shard usando distintas partition keys.
2. Agregue una segunda Lambda que cuente alertas por contenedor y escriba el resumen en S3.
3. Entregue un informe de una página con: diagrama de arquitectura, decisiones de diseño (provisionado vs on-demand, buffer de Firehose) y estimación de costo.

**Rúbrica sugerida (100 pts)**

| Criterio | Puntos |
|---|---|
| Pipeline funcional (productor → stream → Lambda) | 30 |
| Persistencia en S3 (Firehose o Plan B) | 20 |
| Análisis de partition key y shards | 20 |
| Informe y diagrama | 20 |
| Limpieza de recursos evidenciada | 10 |

---

## 5. Preguntas de repaso

1. ¿Qué garantiza el orden de los registros en Kinesis, y hasta qué nivel?
2. ¿Por qué Firehose no es un servicio de procesamiento en tiempo real?
3. ¿Qué ocurre con los datos tras 24 horas de retención?
4. Diferencie `TRIM_HORIZON` de `LATEST`.
5. ¿Cuándo preferiría modo on-demand sobre provisionado?
6. ¿Qué significa entrega "al menos una vez" y cómo lo mitiga el consumidor?
7. ¿Cuántos shards necesita un flujo de 3 MB/s y 2500 registros/s?
   *(Respuesta: 3 por volumen y 3 por registros → mínimo 3.)*

---

## 6. Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `AccessDenied` al crear Firehose o roles | El Learner Lab no permite crear roles IAM | Usar `LabRole` existente |
| `ExpiredToken` | Credenciales del lab caducadas | Reiniciar el lab y copiar credenciales nuevas |
| `ProvisionedThroughputExceededException` | Se excede 1 MB/s o 1000 reg/s por shard | Reintentar con *backoff* o añadir shards |
| Lambda no se dispara | Trigger deshabilitado o *Starting position* en `Latest` sin datos nuevos | Enviar más datos tras crear el trigger |
| S3 vacío tras Firehose | Buffer aún no se cumple | Esperar > 60 s o reducir el buffer |
| Servicio no aparece en la consola | No habilitado en ese Learner Lab | Aplicar el plan B y avisar al administrador del curso |

---

## 7. Costos y buenas prácticas

- Un shard provisionado cuesta del orden de centavos de dólar por hora; el costo real del taller es muy bajo **si se elimina el stream**.
- No deje streams, triggers ni delivery streams activos entre sesiones.
- Use nombres con prefijo común (`taller-kinesis-`) para localizar y borrar todo fácilmente.
