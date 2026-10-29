"""Reto final: segunda Lambda que cuenta alertas por contenedor y escribe el resumen en S3.

Handler: contar_alertas.lambda_handler
Variable de entorno: BUCKET (y UMBRAL opcional, por defecto 8.0)
Trigger: Kinesis taller-kinesis-sensores (batch 500, ventana 60 s, Trim horizon).

Lambda no guarda estado entre invocaciones, así que cada lote produce un resumen
parcial en resumen-alertas/anio=YYYY/mes=MM/dia=DD/. El total por contenedor se obtiene
sumando los resúmenes en Athena (ver sql/athena.sql). La clave usa los sequence numbers
del lote, igual que el Plan B, para que un reintento no duplique conteos.
"""
import base64
import json
import os
from collections import Counter
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
UMBRAL = float(os.environ.get("UMBRAL", "8.0"))


def lambda_handler(event, context):
    bucket = os.environ["BUCKET"]
    eventos, alertas, maximas = Counter(), Counter(), {}
    for registro in event["Records"]:
        payload = base64.b64decode(registro["kinesis"]["data"]).decode("utf-8")
        for linea in payload.strip().splitlines():
            e = json.loads(linea)
            c = e["contenedor_id"]
            eventos[c] += 1
            maximas[c] = max(maximas.get(c, e["temperatura"]), e["temperatura"])
            if e["temperatura"] > UMBRAL:
                alertas[c] += 1

    ahora = datetime.now(timezone.utc)
    filas = [
        {
            "contenedor_id": c,
            "eventos": eventos[c],
            "alertas": alertas[c],
            "temp_max": maximas[c],
            "umbral": UMBRAL,
            "procesado": ahora.isoformat(),
        }
        for c in sorted(eventos)
    ]

    shard = event["Records"][0]["eventID"].split(":")[0]
    primero = event["Records"][0]["kinesis"]["sequenceNumber"]
    ultimo = event["Records"][-1]["kinesis"]["sequenceNumber"]
    clave = (f"resumen-alertas/anio={ahora:%Y}/mes={ahora:%m}/dia={ahora:%d}/"
             f"{shard}-{primero}-{ultimo}.json")
    cuerpo = "".join(json.dumps(f) + "\n" for f in filas)
    s3.put_object(Bucket=bucket, Key=clave, Body=cuerpo.encode("utf-8"), ContentType="application/x-ndjson")
    print(f"s3://{bucket}/{clave}: {sum(alertas.values())} alertas en {sum(eventos.values())} eventos")
    return {"batchItemFailures": []}
