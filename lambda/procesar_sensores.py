"""Lab 4: Lambda consumidora del stream. Imprime alertas en CloudWatch Logs.

Handler: procesar_sensores.lambda_handler
Trigger: Kinesis taller-kinesis-sensores (batch 100, ventana 5 s, Latest,
         ReportBatchItemFailures activado).
"""
import base64
import json

UMBRAL = 8.0  # °C


def lambda_handler(event, context):
    fallidos = []
    alertas = 0
    for registro in event["Records"]:
        try:
            payload = base64.b64decode(registro["kinesis"]["data"]).decode("utf-8")
            for linea in payload.strip().splitlines():
                e = json.loads(linea)
                if e["temperatura"] > UMBRAL:
                    alertas += 1
                    print(f"ALERTA {e['contenedor_id']}: {e['temperatura']} °C ({e['timestamp']})")
                else:
                    print(f"OK {e['contenedor_id']}: {e['temperatura']} °C")
        except (ValueError, KeyError) as err:
            # Respuesta por lotes parcial: Kinesis reintenta desde este registro.
            print(f"ERROR en {registro['kinesis']['sequenceNumber']}: {err}")
            fallidos.append({"itemIdentifier": registro["kinesis"]["sequenceNumber"]})
            break
    print(f"Registros: {len(event['Records'])} | Alertas: {alertas}")
    return {"batchItemFailures": fallidos}
