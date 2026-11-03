# Ejercicios adicionales: productores y consumidores propios

Enunciado: [ejercicios_adicionales_enunciado.pdf](ejercicios_adicionales_enunciado.pdf). Todo se ejecuta en CloudShell (`us-east-1`, `LabRole`) desde la raíz del repositorio (`cd ~/kinesis && git pull`).

**Regla de oro:** al terminar cada ejercicio, borre su stream. Cada shard cuesta unos 0,015 USD/h aunque no se use.

```bash
aws kinesis delete-stream --stream-name <nombre> --enforce-consumer-deletion
```

`scripts/limpieza.sh` también borra `ej-compite`, `ej-reparto`, `ej-reshard` y la tabla `leases`.

| Script | Ejercicio |
|---|---|
| `src/productor_param.py` | 1: `--stream --tasa --duracion --claves --sesgo`. Lotes de hasta 500 y reintento de los fallidos |
| `src/consumidor_shard.py` | 2, 3 y 5: `--stream --indice-shard --pausa --nombre --desde`, más `--duracion`, `--seguir-hijos` y `--verificar` |
| `src/contador_por_shard.py` | 4: registros por shard en barras de texto, `--minutos` para separar corridas y `--csv` |
| `src/consumidor_efo.py` | 6: enhanced fan-out con `SubscribeToShard` |
| `src/consumidor_leases.py` | Reto: grupo de consumidores con leases en DynamoDB |

Para lanzar varios procesos a la vez, se usa `&` y un log por proceso. `wait` espera a que terminen y `grep RESUMEN` saca la línea final de cada uno.

## Ejercicio 1: productor parametrizable

```bash
python3 src/productor_param.py --stream ej-compite --tasa 200 --duracion 30 --claves 8
```

Cada segundo imprime los registros enviados, reintentados y fallidos. Al final imprime una línea `RESUMEN`. El evento conserva el formato del taller (`contenedor_id`, `temperatura`, `humedad`, `timestamp`), y `contenedor_id` es la partition key (`CT-001` … `CT-0NN`, según `--claves`).

**Resultados (1 shard, 8 claves):**

| `--tasa` | Duración | Enviados | Reintentados | Fallidos | Evidencia |
|---|---|---|---|---|---|
| 200 | 30 s | 6000 | 0 | 0 | `ej1-productor.png` |
| 1500 | 10 s | 15 000 | 0 | 0 | `ej1-tasa-1500.png` |
| 5000 | 15 "s" | 70 500 | 33 895 | 4500 (6 %) | `ej1-reintentos.png` |

- **A 1500 registros/s no hubo rechazos**, aunque el límite nominal es 1000/s. Kinesis tolera ráfagas cortas y los eventos son pequeños: unos 120 B, o sea ~180 KB/s, lejos de 1 MB/s.
- **A 5000/s, los segundos 2 a 4 tuvieron fallos parciales:** 241, 3411 y 3243 registros. Se reenviaron solo esos y entraron todos.
- **Desde el segundo 5, la saturación fue sostenida.** Cada "segundo" lleva 3000 reintentados y 500 fallidos: un lote de 500 rechazado en los 6 intentos (6 × 500), que se pierde. Los "segundos" del script duraron varios segundos reales por el backoff, así que la tasa real quedó cerca del límite del shard.
- **Conclusión:** el reintento selectivo recupera los rechazos puntuales, pero ante una sobrecarga sostenida no basta y hay que redimensionar (más shards). Ninguno de los 33 895 rechazos lanzó una excepción.

**¿Por qué no basta con revisar si la llamada lanzó excepción?** `put_records` no es atómico. Si fallan algunos registros, la llamada responde HTTP 200 sin lanzar excepción, con `FailedRecordCount > 0` y un `ErrorCode` en cada posición fallida (`ProvisionedThroughputExceededException` o `InternalFailure`). Si solo se capturan excepciones, esos registros se pierden sin aviso. Por eso el script recorre `Records` y reenvía solo las posiciones con `ErrorCode`.

**¿Qué pasa con el orden al reintentar?** Se puede romper. Si en un lote falla el registro 2 de `CT-001` y se acepta el 3, el 2 se reenvía después y recibe un *sequence number* mayor que el 3. El consumidor verá 3 antes que 2. Kinesis solo garantiza el orden de llegada dentro del shard. Si el orden por clave importa, hay tres opciones: enviar esa clave en serie con `put_record` y `SequenceNumberForOrdering`, no enviar lo siguiente de esa clave hasta confirmar lo anterior, o incluir un número de secuencia propio en el evento y reordenar en el consumidor.

## Ejercicio 2: consumidores que compiten por un shard

```bash
aws kinesis create-stream --stream-name ej-compite --shard-count 1
aws kinesis wait stream-exists --stream-name ej-compite

# Corrida C (3 consumidores, pausa 0.4). A: un solo consumidor con pausa 1.0. B: tres con pausa 1.0.
for n in 1 2 3; do
  python3 src/consumidor_shard.py --stream ej-compite --pausa 0.4 --nombre c$n --duracion 330 > c$n.log &
done
python3 src/productor_param.py --stream ej-compite --tasa 200 --duracion 300 --claves 8
wait; grep RESUMEN c*.log
```

Los consumidores arrancan antes que el productor y leen desde `LATEST`. Para una prueba rápida basta con `--duracion 60` en el productor y 90 en los consumidores. Conserve `ej-compite` si va a hacer el Ejercicio 6. Si no, bórrelo al terminar.

Resultados con productor a 200 registros/s durante 60 s (12 000 registros):

| Corrida | Consumidores | `--pausa` | Llamadas/s totales | Registros por consumidor | Registros/s (c1 / c2 / c3) | Errores de límite (c1 / c2 / c3) |
|---|---|---|---|---|---|---|
| A | 1 | 1.0 | 1 | 11 918 | 200,5 | 0 |
| B | 3 | 1.0 | 3 | 12 000 cada uno | 201,5 / 201,1 / 205,4 | 0 / 0 / 0 |
| C | 3 | 0.4 | ≈ 7,5 (> 5) | 12 000 cada uno | 200,4 / 201,3 / 202,3 | **28 / 24 / 28** |

Observaciones:
- **En la corrida A faltaron 82 registros.** El productor arrancó antes de que el consumidor abriera su iterador `LATEST`. Desde la corrida B se añade un `sleep 3` antes del productor.
- **La primera corrida C mostró 0 errores**, con c3 a solo 174 registros/s (`ej2-corrida-c-reintentos-ocultos.png`), mientras CloudWatch marcaba `ReadProvisionedThroughputExceeded` en un promedio de 0,18 (`ej2-metrica-read-exceeded.png`). La causa: boto3 reintenta `ProvisionedThroughputExceededException` en silencio, hasta 5 intentos con backoff. `consumidor_shard.py` ahora usa `Config(retries={"total_max_attempts": 1})` para que cada rechazo llegue al script. Con ese cambio, la corrida C repetida muestra 80 rechazos en total (`ej2-corrida-c.png`). Lección: el SDK puede esconder la saturación, así que también hay que mirar CloudWatch.
- **Incluso con rechazos, cada consumidor recibió los 12 000 registros.** El rechazo solo retrasa la lectura; no pierde datos mientras el consumidor reintente y los registros sigan retenidos.

**En la corrida B, ¿recibe cada consumidor todos los registros o solo una parte?** Todos: 12 000 cada uno, a unos 200/s. `GetRecords` no consume ni borra: cada consumidor tiene su propio iterador y lee el shard completo. Lo único que comparten es la capacidad de lectura: 5 llamadas/s y 2 MB/s por shard.

**¿Sirven 3 consumidores sobre 1 shard para dividir el trabajo?** No. Cada uno procesa los mismos 200 eventos/s, así que el trabajo se triplica en lugar de repartirse. Además se estorban: con pausa 0.4 hacen unas 7,5 llamadas/s, por encima del límite de 5, y aparecen los `ProvisionedThroughputExceededException` de la corrida C. Para repartir hace falta un shard por consumidor (Ejercicio 3).

**¿Qué métrica muestra la saturación?** `ReadProvisionedThroughputExceeded` del stream en CloudWatch. En la corrida C llegó a un promedio de 0,18, es decir, unas 18 de cada 100 llamadas `GetRecords` rechazadas. También sube `GetRecords.IteratorAgeMilliseconds` si los consumidores se quedan atrás.

## Ejercicio 3: un shard por consumidor

```bash
aws kinesis create-stream --stream-name ej-reparto --shard-count 3
aws kinesis wait stream-exists --stream-name ej-reparto

for i in 0 1 2; do
  python3 src/consumidor_shard.py --stream ej-reparto --indice-shard $i --pausa 0.4 --nombre c$i --duracion 150 > c$i.log &
done
python3 src/productor_param.py --stream ej-reparto --tasa 300 --duracion 120 --claves 30
wait; grep RESUMEN c*.log
```

No borre `ej-reparto` todavía: el Ejercicio 4 lo reutiliza.

Resultados con productor a 300 registros/s durante 60 s (18 000 registros, 30 claves), en `ej3-reparto.png`:

| Consumidor | Shard | Registros | % | Registros/s | Errores de límite | Claves en el shard (MD5) |
|---|---|---|---|---|---|---|
| c0 | 000 | 7127 | 39,6 % | 121,0 | 0 | 12 |
| c1 | 001 | 2959 | 16,4 % | 50,0 | 0 | 5 |
| c2 | 002 | 7914 | 44,0 % | 135,0 | 0 | 13 |
| **Suma** | | **18 000** | | **≈ 306** | **0** | 30 |

- **Los tres suman exactamente los 18 000 enviados.** Cada registro lo procesó un solo consumidor, así que esta vez el trabajo sí se repartió.
- **0 errores de límite, frente a 80 en la corrida C del Ejercicio 2.** La pausa es la misma (0.4), pero cada shard recibe solo unas 2,5 llamadas/s.
- **El reparto no es parejo (40 / 16 / 44 %),** aunque el enunciado esperaba tasas similares. Al aplicar MD5 a `CT-001` … `CT-030` y repartir los resultados en 3 rangos iguales, quedan 12, 5 y 13 claves por shard. Eso predice 7200 / 3000 / 7800 registros, casi lo medido. Con solo 30 claves el hash todavía no promedia bien. Con cientos de claves el reparto se acercaría al 33 %.
- **El archivo `c3.log` que aparece en el `grep` sobra:** es el resultado viejo del Ejercicio 2. Conviene borrar los logs entre corridas con `rm -f *.log`.

**¿Cuántos consumidores útiles admite un grupo con 3 shards?** Tres, uno por shard. Un cuarto no tiene un shard propio. Si se le asigna uno ya ocupado, procesa los mismos registros que otro (duplica el trabajo) y compite con él por la lectura. KCL lo dejaría sin lease, de reserva, hasta que otro consumidor caiga.

**¿Por qué importa la partition key para repartir parejo?** Kinesis aplica MD5 a la key y la ubica en el rango de hash de un shard. La carga de cada consumidor es la de su shard. Con pocas claves (8 en el taller), el hash puede dejar 4, 3 y 1 claves por shard. Ni siquiera 30 claves bastaron: quedaron 12, 5 y 13 por shard, y el consumidor c1 trabajó menos de la mitad que c2. Con muchas más claves (cientos o miles), el reparto se acerca al 33 % por shard. Una clave muy frecuente concentra la carga en un shard, sin importar cuántos haya (Ejercicio 4).

## Ejercicio 4: hot shard

```bash
python3 src/productor_param.py --stream ej-reparto --tasa 300 --duracion 60 --claves 30 --sesgo 0
python3 src/contador_por_shard.py --stream ej-reparto --minutos 1.5 --csv corrida_a.csv
# Espere 2 minutos para que la ventana no mezcle las corridas
python3 src/productor_param.py --stream ej-reparto --tasa 300 --duracion 60 --claves 30 --sesgo 0.8
python3 src/contador_por_shard.py --stream ej-reparto --minutos 1.5 --csv corrida_b.csv
aws kinesis delete-stream --stream-name ej-reparto --enforce-consumer-deletion
```

`--minutos` lee con `AT_TIMESTAMP` solo la última corrida. Sin esa opción, `TRIM_HORIZON` sumaría también las corridas anteriores. El contador dibuja las barras en texto y guarda un CSV para el gráfico de barras del entregable.

| Shard | Corrida A (sesgo 0) | Corrida B (sesgo 0.8) |
|---|---|---|
| 0 | _pendiente_ | _pendiente_ |
| 1 | _pendiente_ | _pendiente_ |
| 2 | _pendiente_ | _pendiente_ |

**¿Qué porcentaje recibe el shard caliente en la corrida B?** En teoría, el 80 % va siempre a `CT-001`. El 20 % restante se reparte entre las 30 claves, y alrededor de un tercio cae en el mismo shard que `CT-001`. Eso da unos 80 + 20/3 ≈ **87 %**. El valor real depende de cuántas de las 30 claves comparten shard con `CT-001`. Con tasa 300, el shard caliente recibe unos 260 registros/s y los otros dos unos 20 cada uno: los 3 shards rinden como 1.

**Dos soluciones y qué orden se pierde:**
1. **Sufijo aleatorio (*salting*):** usar `CT-001#0` … `CT-001#9` como key. La clave caliente se reparte entre varios shards. Se pierde el orden global de `CT-001`: solo se mantiene dentro de cada sufijo. El consumidor tiene que juntar los sub-flujos, por ejemplo agregando por ventana de tiempo.
2. **Cambiar la granularidad de la clave:** usar una clave más fina, como `contenedor_id + sensor_id`, en lugar de solo `contenedor_id`. El orden se mantiene por sensor, pero ya no entre sensores del mismo contenedor. Una clave más gruesa haría lo contrario: más orden y peor reparto.

## Ejercicio 5: reshard en caliente

```bash
aws kinesis create-stream --stream-name ej-reshard --shard-count 2
aws kinesis wait stream-exists --stream-name ej-reshard
python3 src/productor_param.py --stream ej-reshard --tasa 50 --duracion 240 --claves 8 > prod.log &
sleep 90
aws kinesis update-shard-count --stream-name ej-reshard --target-shard-count 4 --scaling-type UNIFORM_SCALING
aws kinesis wait stream-exists --stream-name ej-reshard      # vuelve a ACTIVE
aws kinesis list-shards --stream-name ej-reshard --output table \
  --query "Shards[].{id:ShardId,padre:ParentShardId,fin:SequenceNumberRange.EndingSequenceNumber}"
wait   # a que termine el productor
for i in 0 1; do
  python3 src/consumidor_shard.py --stream ej-reshard --indice-shard $i --desde TRIM_HORIZON \
    --seguir-hijos --verificar --pausa 0.3 --nombre r$i --duracion 60 > r$i.log &
done
wait; grep RESUMEN prod.log r*.log
```

Los shards **cerrados** son los padres, `000` y `001`: tienen `EndingSequenceNumber`. Los **abiertos** son los hijos: no lo tienen y muestran un `ParentShardId`. `--seguir-hijos` lee el padre hasta que `NextShardIterator` llega vacío y entonces sigue con sus hijos. `--verificar` cuenta las veces que el `timestamp` de una clave retrocede. La suma de registros de `r0` y `r1` debe ser igual a los enviados del productor.

| | Valor |
|---|---|
| Enviados por el productor | _pendiente_ |
| Leídos por r0 + r1 | _pendiente_ |
| Inversiones de orden por clave | _pendiente_ (esperado: 0) |

**¿Qué pasa con los datos que ya estaban en los padres?** Se quedan donde estaban. El padre se cierra a nuevas escrituras, pero sigue legible hasta que vence la retención (24 h). Lo nuevo va a los hijos. No se mueve ni se copia ningún registro.

**¿Por qué leer el padre hasta el final antes que los hijos?** Los registros de una clave anteriores al reshard están en el padre y los posteriores en un hijo. Si se lee el hijo primero, se procesan eventos nuevos antes que los viejos de la misma clave y se rompe el orden. En un *merge* el hijo tiene dos padres, así que hay que terminar ambos (`consumidor_shard.py` lo comprueba con `padres()`).

**¿Cuándo usar `split_shard` en vez de `update_shard_count`?** Cuando el problema es un shard concreto, como el caliente del Ejercicio 4. `split_shard` divide solo ese shard y permite elegir el hash donde se corta. `update_shard_count` escala todos los shards de forma uniforme, puede crear shards intermedios, solo puede ir a la mitad o al doble y tiene un límite de llamadas por periodo. Sirve para crecer en general, no para un shard concreto.

## Ejercicio 6 (opcional): enhanced fan-out

```bash
aws kinesis create-stream --stream-name ej-compite --shard-count 1     # si ya lo borró
aws kinesis wait stream-exists --stream-name ej-compite
for n in 1 2; do
  python3 src/consumidor_efo.py --stream ej-compite --nombre efo-$n --duracion 120 > efo-$n.log &
done
sleep 30   # registro de los consumidores hasta ACTIVE
python3 src/productor_param.py --stream ej-compite --tasa 200 --duracion 60 --claves 8
wait; grep -h -E "RESUMEN|Exception|AccessDenied" efo-*.log
aws kinesis delete-stream --stream-name ej-compite --enforce-consumer-deletion
```

El script registra el consumidor, espera a que esté `ACTIVE`, se suscribe con `subscribe_to_shard`, renueva la suscripción cada unos 5 minutos y al terminar elimina el consumidor. Si el Learner Lab no permite `RegisterStreamConsumer` o `SubscribeToShard` (`AccessDeniedException`), el ejercicio queda en el análisis teórico.

| Consumidor | Registros/s | Errores |
|---|---|---|
| efo-1 | _pendiente_ | _pendiente_ |
| efo-2 | _pendiente_ | _pendiente_ |

**Resultado esperado:** sin errores de límite. Cada consumidor tiene 2 MB/s dedicados por shard y recibe los registros por push (HTTP/2), sin repartirse el límite de 5 `GetRecords`/s. La latencia también baja, de unos 200 ms con *polling* a unos 70 ms. A cambio, cobra por consumidor-shard-hora y por GB leído.

## Reto final: grupo de consumidores con leases en DynamoDB

Se usa `ej-reshard`, que tras el Ejercicio 5 tiene **4 shards abiertos**. Hay que mantener el productor enviando y abrir dos pestañas de CloudShell, una por consumidor.

```bash
python3 src/productor_param.py --stream ej-reshard --tasa 40 --duracion 900 --claves 30 > prod.log &
python3 src/consumidor_leases.py --stream ej-reshard --nombre w1      # pestaña 1
python3 src/consumidor_leases.py --stream ej-reshard --nombre w2      # pestaña 2
```

Cómo funciona:
- **Tabla `leases`**, clave `shard_id`, con los atributos `propietario`, `expira_en` (epoch ms) y `checkpoint`. Se crea en modo *on-demand* si no existe. Cada consumidor guarda también un latido (`worker#w1`) para saber cuántos hay vivos.
- **Adueñarse de un lease:** `update_item` con `ConditionExpression = attribute_not_exists(shard_id) OR expira_en < :ahora OR propietario = :yo`. Si dos lo intentan a la vez, DynamoDB acepta solo una escritura y la otra recibe `ConditionalCheckFailedException`.
- **Reparto:** cada consumidor toma como mucho ⌈shards abiertos ÷ consumidores vivos⌉ leases y suelta los que le sobran (`expira_en = 0`). Así, cuando llega `w2`, `w1` le cede 2 de sus 4.
- **Renovación y checkpoint:** cada `ttl/3` segundos renueva sus leases. Guarda el `checkpoint` cada 100 registros. Las dos escrituras llevan la condición `propietario = :yo`, así que un consumidor que perdió el lease no pisa al nuevo dueño.
- **Al reanudar** lee con `AFTER_SEQUENCE_NUMBER` desde el checkpoint. Si no hay checkpoint, usa `TRIM_HORIZON`.

| Prueba | Cómo | Resultado esperado | Resultado |
|---|---|---|---|
| Reparto | Arrancar w1 y, 20 s después, w2 | w1 toma 4; con w2 vivo quedan 2 y 2 | _pendiente_ |
| Caída | `kill -9` al proceso de w2 (`pgrep -f "nombre w2"`) | Tras ~15 s (ttl) vencen sus leases y w1 vuelve a tener 4 | _pendiente_ |
| Reinicio | Volver a lanzar w2 | Recupera 2 shards y sigue desde el checkpoint, con como mucho 100 registros repetidos por shard | _pendiente_ |

Con Ctrl+C, el consumidor guarda los checkpoints y suelta sus leases (salida ordenada). `kill -9` simula una caída real: no suelta nada y los leases tienen que vencer. Los registros que se repiten tras un reinicio son los procesados después del último checkpoint. Es la entrega *al menos una vez*, por eso el procesamiento debe ser idempotente.

Limpieza del reto:

```bash
aws kinesis delete-stream --stream-name ej-reshard --enforce-consumer-deletion
aws dynamodb delete-table --table-name leases
```

O bien `bash scripts/limpieza.sh`, que borra los tres streams `ej-*` y la tabla.
