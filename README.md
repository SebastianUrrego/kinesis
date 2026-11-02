# Taller: Streaming de datos con Amazon Kinesis (AWS Academy Learner Lab)

Curso: **Big Data e Ingeniería de Datos**. Guía completa del taller: [taller_aws_kinesis_aws_academy.md](taller_aws_kinesis_aws_academy.md).

Pipeline de streaming para monitorear contenedores refrigerados de una empresa de logística. Los sensores envían eventos JSON (temperatura y humedad). El pipeline genera alertas cuando la temperatura supera **8 °C** y guarda todos los eventos en un data lake en S3.

```
Productor (Python) ─► Kinesis Data Stream ─┬─► Lambda (alertas, CloudWatch Logs)
                                           └─► Firehose ─► S3 ─► (Athena)
```

## Entorno

| Elemento | Valor |
|---|---|
| Cuenta | AWS Academy Learner Lab (usuario `voclabs`) |
| Región | `us-east-1` (N. Virginia) |
| Rol IAM | `LabRole` (no se pueden crear roles propios) |
| Terminal | CloudShell o Cloud9 (Python 3 + `boto3`) |
| Sesión | Unas 4 h. Las credenciales temporales caducan al terminar |

## Recursos de AWS

| Recurso | Nombre |
|---|---|
| Kinesis Data Stream | `taller-kinesis-sensores` (provisionado, 1 shard) |
| Lambda | `procesar-sensores` (Python 3.x, rol `LabRole`) |
| Bucket S3 | `taller-kinesis-<iniciales>-<número>` |
| Firehose | Origen: el stream. Destino: S3. Prefijo `sensores/anio=…/mes=…/dia=…/`. Buffer de 1 MB o 60 s |
| Tabla Athena (opcional) | `sensores` sobre `s3://<bucket>/sensores/` |

## Estructura del repositorio

```
kinesis/
├── README.md
├── CLAUDE.md
├── taller_aws_kinesis_aws_academy.md   # enunciado
├── requirements.txt                    # boto3 (ya viene en CloudShell)
├── src/
│   ├── productor.py                    # Lab 2: put_records con reintentos; --key-constante
│   └── consumidor.py                   # Lab 3: GetRecords en todos los shards; --tipo, --contar
├── lambda/
│   ├── procesar_sensores.py            # Lab 4: alertas > 8 °C a CloudWatch Logs
│   ├── plan_b_s3_writer.py             # Plan B del Lab 5: stream → S3 sin Firehose
│   └── contar_alertas.py               # Reto: resumen de alertas por contenedor → S3
├── sql/
│   └── athena.sql                      # Tablas sensores y resumen_alertas, y consultas
├── scripts/
│   ├── crear_recursos.sh               # Crea stream, bucket, Lambdas, triggers y Firehose
│   └── limpieza.sh                     # Lab 6: borra todo y verifica
├── tests/
│   └── test_local.py                   # Pruebas sin AWS (Lambdas y productor)
└── docs/
    ├── respuestas.md                   # Preguntas de los labs y de repaso
    ├── informe.md                      # Reto final: diagrama, decisiones y costos
    └── evidencias/                     # Capturas de pantalla
```

## Cómo ejecutarlo

En CloudShell (`us-east-1`), con el Learner Lab iniciado:

```bash
git clone https://github.com/SebastianUrrego/kinesis.git
cd kinesis
chmod +x scripts/*.sh
aws sts get-caller-identity
```

**Opción A: todo con la CLI**

```bash
./scripts/crear_recursos.sh taller-kinesis-<iniciales>-<número>        # stream + Lambda + Firehose
python3 src/productor.py                                               # Lab 2
python3 src/consumidor.py --tipo TRIM_HORIZON                          # Lab 3 (Ctrl+C para salir)
```

Si Firehose no se puede crear con `LabRole`, el script lo avisa. En ese caso use el Plan B:

```bash
./scripts/crear_recursos.sh taller-kinesis-<iniciales>-<número> planb
```

**Opción B: por consola**, siguiendo los pasos del enunciado. En la Lambda pegue el código de `lambda/procesar_sensores.py` y elija el rol `LabRole`.

Cómo verificar cada parte:
- Lab 2: pestaña *Monitoring* del stream, métrica `IncomingRecords`.
- Lab 4: CloudWatch, log group `/aws/lambda/procesar-sensores`, líneas `ALERTA` y `OK`.
- Lab 5: pasados más de 60 s, objetos en `s3://<bucket>/sensores/anio=…/mes=…/dia=…/`.
- Athena: en *Settings*, ponga como ubicación de resultados `s3://<bucket>/athena-resultados/` y ejecute `sql/athena.sql`.

**Reto final** (3 shards, partition keys y Lambda de conteo):

```bash
SHARDS=3 RETO=1 ./scripts/crear_recursos.sh taller-kinesis-<iniciales>-<número>
python3 src/productor.py --eventos 1000
python3 src/consumidor.py --tipo AT_TIMESTAMP --minutos 2 --contar
python3 src/productor.py --eventos 1000 --key-constante
python3 src/consumidor.py --tipo AT_TIMESTAMP --minutos 2 --contar
```

Por consola, el número de shards solo puede ir entre la mitad y el doble en cada cambio: de 1 a 3 hay que pasar por 2.

Los resultados se anotan en [docs/informe.md](docs/informe.md).

**Ejercicios adicionales** (productor parametrizable, consumidores que compiten o se reparten shards, hot shard, reshard, enhanced fan-out y leases en DynamoDB): ver [docs/ejercicios.md](docs/ejercicios.md).

**Pruebas locales** (sin AWS):

```bash
python -m unittest discover -s tests -v
```

## Limpieza

Un stream de Kinesis **sigue consumiendo crédito** aunque termine la sesión del lab. Al final de cada sesión:

```bash
./scripts/limpieza.sh taller-kinesis-<iniciales>-<número>
```

El script borra las Lambdas y sus triggers, Firehose, el stream y el bucket, y al final muestra una verificación que debe salir vacía. Después pulse **End Lab**.

## Entregables (reto final, 100 pts)

| Criterio | Puntos |
|---|---|
| Pipeline funcional (productor → stream → Lambda) | 30 |
| Persistencia en S3 (Firehose o Plan B) | 20 |
| Análisis de partition key y shards (3 shards) | 20 |
| Informe de una página y diagrama | 20 |
| Limpieza de recursos evidenciada | 10 |

Extensiones del reto:
- Usar 3 shards y comparar cómo se reparten los registros con distintas partition keys.
- Agregar una segunda Lambda que cuente las alertas por contenedor y escriba el resumen en S3.
- Escribir un informe con la arquitectura, las decisiones (provisionado frente a on-demand, buffer de Firehose) y una estimación de costo.

## Solución de problemas

| Síntoma | Solución |
|---|---|
| `AccessDenied` al crear roles o Firehose | Usar el `LabRole` existente |
| `ExpiredToken` | Reiniciar el lab y copiar credenciales nuevas desde *AWS Details* |
| `ProvisionedThroughputExceededException` | Reintentar con *backoff* o añadir shards |
| La Lambda no se dispara | Enviar datos nuevos después de crear el trigger (posición *Latest*) |
| S3 vacío | Esperar más de 60 s (buffer de Firehose) |
| Script falla de forma extraña en Linux | Revisar finales de línea CRLF (`cat -A`, `sed -i 's/\r$//'`) |
