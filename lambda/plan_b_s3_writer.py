"""Plan B del Lab 5: persiste el stream en S3 sin Firehose.

Handler: plan_b_s3_writer.lambda_handler
Variable de entorno: BUCKET
Trigger: Kinesis taller-kinesis-sensores (batch 100, ventana 60 s, Trim horizon).

Escribe un objeto JSON Lines por lote y por día, con el mismo prefijo que Firehose
(sensores/anio=YYYY/mes=MM/dia=DD/) para que la tabla de Athena sirva igual.
La clave del objeto se deriva de los sequence numbers del lote: si Kinesis reintenta
el mismo lote, se sobrescribe el mismo objeto en vez de duplicar datos (idempotencia).
"""
import base64
import json
import os
from collections import defaultdict

import boto3

s3 = boto3.client("s3")
PREFIJO = "sensores"


def particion(timestamp):
    # timestamp ISO 8601, p. ej. 2026-10-28T15:04:05.123456+00:00
    anio, mes, dia = timestamp[:10].split("-")
    return f"{PREFIJO}/anio={anio}/mes={mes}/dia={dia}"


def lambda_handler(event, context):
    bucket = os.environ["BUCKET"]
    lineas_por_particion = defaultdict(list)
    for registro in event["Records"]:
        payload = base64.b64decode(registro["kinesis"]["data"]).decode("utf-8")
        for linea in payload.strip().splitlines():
            e = json.loads(linea)
            lineas_por_particion[particion(e["timestamp"])].append(json.dumps(e))

    # eventID = "shardId-000000000000:<sequenceNumber>"
    shard = event["Records"][0]["eventID"].split(":")[0]
    primero = event["Records"][0]["kinesis"]["sequenceNumber"]
    ultimo = event["Records"][-1]["kinesis"]["sequenceNumber"]

    for prefijo, lineas in lineas_por_particion.items():
        clave = f"{prefijo}/{shard}-{primero}-{ultimo}.json"
        s3.put_object(Bucket=bucket, Key=clave, Body=("\n".join(lineas) + "\n").encode("utf-8"),
                      ContentType="application/x-ndjson")
        print(f"s3://{bucket}/{clave} ({len(lineas)} eventos)")
    return {"batchItemFailures": []}
