"""Lab 2: productor de eventos de sensores hacia Kinesis Data Streams.

Uso:
    python3 src/productor.py                       # 200 eventos, key = contenedor_id
    python3 src/productor.py --eventos 1000        # más carga
    python3 src/productor.py --key-constante       # experimento: todos al mismo shard
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

import boto3

STREAM = "taller-kinesis-sensores"
REGION = "us-east-1"
CONTENEDORES = [f"CT-{i:03d}" for i in range(1, 9)]
TAMANO_LOTE = 10
MAX_REINTENTOS = 5


def generar_evento():
    return {
        "contenedor_id": random.choice(CONTENEDORES),
        "temperatura": round(random.gauss(5, 3), 2),   # °C
        "humedad": round(random.uniform(40, 90), 1),   # %
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def a_registro(evento, key_constante):
    return {
        # Una línea JSON por registro: Firehose concatena registros y Athena separa por "\n".
        "Data": (json.dumps(evento) + "\n").encode("utf-8"),
        "PartitionKey": "constante" if key_constante else evento["contenedor_id"],
    }


def enviar_lote(kinesis, stream, registros):
    """Envía con put_records y reintenta con backoff solo los registros fallidos.

    Devuelve (enviados, perdidos). Un fallo típico es ProvisionedThroughputExceededException
    cuando se supera 1 MB/s o 1000 registros/s en un shard.
    """
    pendientes = registros
    enviados = 0
    for intento in range(MAX_REINTENTOS):
        respuesta = kinesis.put_records(StreamName=stream, Records=pendientes)
        fallidos = [reg for reg, res in zip(pendientes, respuesta["Records"]) if "ErrorCode" in res]
        enviados += len(pendientes) - len(fallidos)
        if not fallidos:
            return enviados, 0
        errores = {res["ErrorCode"] for res in respuesta["Records"] if "ErrorCode" in res}
        espera = (2 ** intento) * 0.1
        print(f"  {len(fallidos)} fallidos ({', '.join(errores)}); reintento en {espera:.1f} s")
        time.sleep(espera)
        pendientes = fallidos
    return enviados, len(pendientes)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", default=STREAM)
    parser.add_argument("--eventos", type=int, default=200, help="total de eventos a enviar")
    parser.add_argument("--pausa", type=float, default=1.0, help="segundos entre lotes")
    parser.add_argument("--key-constante", action="store_true",
                        help="usa la misma PartitionKey para todo (hot shard)")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    enviados = perdidos = 0
    while enviados + perdidos < args.eventos:
        n = min(TAMANO_LOTE, args.eventos - enviados - perdidos)
        lote = [a_registro(generar_evento(), args.key_constante) for _ in range(n)]
        ok, mal = enviar_lote(kinesis, args.stream, lote)
        enviados += ok
        perdidos += mal
        print(f"Enviados: {enviados} | Perdidos: {perdidos}")
        time.sleep(args.pausa)


if __name__ == "__main__":
    main()
