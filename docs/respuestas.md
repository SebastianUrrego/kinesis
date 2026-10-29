# Respuestas del taller

## Preguntas de los laboratorios

### Lab 1: ¿cuál es la capacidad máxima de escritura del stream?

Con 1 shard provisionado: **1 MB/s o 1000 registros/s**, lo que se alcance primero. La lectura llega a 2 MB/s compartidos entre los consumidores estándar.

### Lab 2: ¿qué implica una `PartitionKey` constante con varios shards?

Kinesis aplica un hash MD5 a la partition key para elegir el shard. Si la key es constante, todos los registros van al **mismo shard** (*hot shard*):
- El throughput real queda limitado a 1 MB/s o 1000 reg/s, aunque el stream tenga N shards. El resto de shards se paga y no se usa.
- Aparecen antes los `ProvisionedThroughputExceededException`.
- Los consumidores tampoco se paralelizan, porque un único shard concentra todo el trabajo.

Con `contenedor_id` como key, los 8 contenedores se reparten entre los shards y el orden se mantiene **por contenedor**. Para medirlo se usa `python3 src/consumidor.py --contar` (ver el informe).

### Lab 3: `LATEST`, `TRIM_HORIZON` y `AT_TIMESTAMP`

| Tipo | Empieza a leer desde |
|---|---|
| `TRIM_HORIZON` | El registro más antiguo aún retenido (hasta 24 h por defecto). Sirve para reprocesar (*replay*) |
| `LATEST` | Justo después del último registro. Solo ve lo que llegue después de abrir el iterador |
| `AT_TIMESTAMP` | El primer registro con marca de llegada igual o posterior a la fecha dada (`--minutos N` en el script) |

También existen `AT_SEQUENCE_NUMBER` y `AFTER_SEQUENCE_NUMBER`, que sirven para reanudar desde un checkpoint.

### Lab 3: dos consumidores a la vez

Sí, **ambos reciben los mismos registros**. Kinesis no borra un registro al leerlo: cada consumidor lleva su propio iterador. Pero los consumidores estándar **comparten** el límite de lectura del shard: 2 MB/s y 5 llamadas `GetRecords`/s. Con más consumidores, cada uno recibe menos ancho de banda y aumenta la latencia. La solución es **enhanced fan-out**, que da 2 MB/s dedicados por consumidor mediante `SubscribeToShard` (HTTP/2 push).

### Lab 4: *Batch size* 5 y *Batch window* 0 frente a 100 y 5 s

El productor envía unos 10 eventos/s.
- **100 / 5 s:** Lambda acumula hasta 5 s. Llegan unos 50 registros por invocación, así que 200 eventos dan unas **4 o 5 invocaciones**.
- **5 / 0 s:** invoca en cuanto hay registros, con 5 como máximo. 200 eventos dan **40 invocaciones o más**.

Más invocaciones implican más costo por petición y más *overhead*, a cambio de menor latencia. La métrica `Invocations` de la Lambda en CloudWatch muestra la diferencia.

### Lab 4: ¿por qué la Lambda debe ser idempotente?

La entrega es *al menos una vez*. Si la Lambda falla, sufre un *timeout* o reporta `batchItemFailures`, Kinesis **reenvía el lote** (o reintenta desde el registro fallido). Además, el productor puede duplicar registros al reintentar un `put_records`. Procesar dos veces el mismo evento no debe cambiar el resultado. En este repositorio:
- `procesar_sensores.py` solo escribe logs, así que repetir no causa daño.
- `plan_b_s3_writer.py` y `contar_alertas.py` usan como clave de S3 los *sequence numbers* del lote. Un reintento sobrescribe el mismo objeto y no duplica datos ni conteos.

## Preguntas de repaso

1. **¿Qué garantiza el orden y hasta qué nivel?** La partition key. Los registros con la misma key van al mismo shard y conservan el orden por `SequenceNumber` **dentro del shard**. No hay orden global entre shards.
2. **¿Por qué Firehose no es tiempo real?** Porque entrega por **buffer**: espera a que se cumpla el tamaño (1 MB) o el intervalo (60 s), lo que ocurra primero. La latencia es de decenas de segundos a minutos, es decir, *casi* tiempo real.
3. **¿Qué ocurre tras 24 h de retención?** Los registros expiran y se eliminan del stream, sin posibilidad de *replay*. Para conservarlos hay que persistirlos (Firehose/S3) o ampliar la retención hasta 365 días, con costo extra.
4. **`TRIM_HORIZON` frente a `LATEST`:** `TRIM_HORIZON` lee desde el registro más antiguo retenido. `LATEST` lee solo lo que llegue después de crear el iterador.
5. **¿Cuándo usar on-demand en vez de provisionado?** Cuando el tráfico es impredecible, tiene picos o está empezando y no se puede dimensionar, o cuando no se quiere gestionar shards. Con carga estable y sostenida, el provisionado sale más barato (ver el informe).
6. **"Al menos una vez":** cada registro se entrega una o más veces y puede haber duplicados por reintentos del productor o del consumidor. Se mitiga con consumidores **idempotentes**: claves deterministas (*sequence number* o un id del evento), *upserts* en lugar de *inserts*, y deduplicación por id.
7. **Shards para 3 MB/s y 2500 registros/s:** por volumen, ⌈3 / 1⌉ = 3. Por registros, ⌈2500 / 1000⌉ = 3. Se necesitan **al menos 3**, y conviene usar 4 para tener margen.
