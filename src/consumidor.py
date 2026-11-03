"""Lab 3: consumidor manual con GetRecords (lee todos los shards del stream).

Uso:
    python3 src/consumidor.py                              # TRIM_HORIZON, sigue leyendo
    python3 src/consumidor.py --tipo LATEST                # solo registros nuevos
    python3 src/consumidor.py --tipo AT_TIMESTAMP --minutos 10
    python3 src/consumidor.py --contar                     # reto: registros por shard y termina
"""
import argparse
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

STREAM = "taller-kinesis-sensores"
REGION = "us-east-1"


def listar_shards(kinesis, stream):
    shards, token = [], None
    while True:
        kwargs = {"NextToken": token} if token else {"StreamName": stream}
        salida = kinesis.list_shards(**kwargs)
        shards += [s["ShardId"] for s in salida["Shards"]]
        token = salida.get("NextToken")
        if not token:
            return shards


def abrir_iteradores(kinesis, stream, shards, tipo, minutos):
    iteradores = {}
    for shard_id in shards:
        kwargs = {"StreamName": stream, "ShardId": shard_id, "ShardIteratorType": tipo}
        if tipo == "AT_TIMESTAMP":
            kwargs["Timestamp"] = datetime.now(timezone.utc) - timedelta(minutes=minutos)
        iteradores[shard_id] = kinesis.get_shard_iterator(**kwargs)["ShardIterator"]
    return iteradores


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", default=STREAM)
    parser.add_argument("--tipo", default="TRIM_HORIZON", choices=["TRIM_HORIZON", "LATEST", "AT_TIMESTAMP"])
    parser.add_argument("--minutos", type=int, default=5, help="para AT_TIMESTAMP: cuántos minutos atrás")
    parser.add_argument("--contar", action="store_true",
                        help="no imprime eventos; cuenta registros por shard y por contenedor, y termina")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    shards = listar_shards(kinesis, args.stream)
    print(f"Shards: {shards}")
    iteradores = abrir_iteradores(kinesis, args.stream, shards, args.tipo, args.minutos)

    por_shard, por_contenedor = Counter(), Counter()
    rondas_vacias = 0
    while iteradores:
        hubo_datos = False
        for shard_id, iterador in list(iteradores.items()):
            salida = kinesis.get_records(ShardIterator=iterador, Limit=500)
            # Un lote vacío no significa fin: si MillisBehindLatest > 0 quedan registros más adelante.
            if salida.get("MillisBehindLatest", 0) > 0:
                hubo_datos = True
            for r in salida["Records"]:
                hubo_datos = True
                for linea in r["Data"].decode("utf-8").strip().splitlines():
                    evento = json.loads(linea)
                    por_shard[shard_id] += 1
                    por_contenedor[(shard_id, evento["contenedor_id"])] += 1
                    if not args.contar:
                        print(shard_id[-3:], r["SequenceNumber"][-6:], r["PartitionKey"], evento)
            # NextShardIterator falta cuando el shard se cerró (p. ej. tras un resharding).
            if salida.get("NextShardIterator"):
                iteradores[shard_id] = salida["NextShardIterator"]
            else:
                del iteradores[shard_id]
        if hubo_datos:
            rondas_vacias = 0
            time.sleep(0.25)   # máximo 5 GetRecords/s por shard
            continue
        rondas_vacias += 1
        # En modo --contar se termina tras ~10 s sin datos nuevos.
        if args.contar and rondas_vacias >= 5:
            break
        # Límite de 5 llamadas GetRecords/s por shard: esperar si no hay datos.
        time.sleep(2)

    if args.contar:
        total = sum(por_shard.values())
        print(f"\nTotal de eventos: {total}")
        for shard_id in shards:
            n = por_shard[shard_id]
            pct = 100 * n / total if total else 0
            contenedores = sorted(c for (s, c) in por_contenedor if s == shard_id)
            print(f"{shard_id}: {n:6d} ({pct:5.1f} %)  contenedores: {', '.join(contenedores) or '-'}")


if __name__ == "__main__":
    main()
