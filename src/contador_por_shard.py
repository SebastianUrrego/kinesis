"""Ejercicio 4: cuenta cuántos registros hay en cada shard (detecta un hot shard).

Uso:
    python3 src/contador_por_shard.py --stream ej-reparto                    # todo, desde TRIM_HORIZON
    python3 src/contador_por_shard.py --stream ej-reparto --minutos 2 --csv corrida_b.csv

Con --minutos lee desde AT_TIMESTAMP (ahora - N minutos) para separar dos corridas en el
mismo stream. Termina tras ~10 s sin datos y dibuja un gráfico de barras en texto.
"""
import argparse
import csv
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

from consumidor_shard import listar_shards

REGION = "us-east-1"


def barras(conteo, orden, ancho=40):
    total = sum(conteo.values()) or 1
    maximo = max(conteo.values(), default=0) or 1
    for clave in orden:
        n = conteo[clave]
        print(f"{clave}  {'#' * round(ancho * n / maximo):<{ancho}}  {n:6d} ({100 * n / total:5.1f} %)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--minutos", type=float, default=0, help="leer solo los últimos N minutos (0 = TRIM_HORIZON)")
    parser.add_argument("--csv", help="guarda shard,registros para graficar en una hoja de cálculo")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    shards = [s["ShardId"] for s in listar_shards(kinesis, args.stream)]
    iteradores = {}
    for shard_id in shards:
        kwargs = {"StreamName": args.stream, "ShardId": shard_id, "ShardIteratorType": "TRIM_HORIZON"}
        if args.minutos:
            kwargs.update(ShardIteratorType="AT_TIMESTAMP",
                          Timestamp=datetime.now(timezone.utc) - timedelta(minutes=args.minutos))
        iteradores[shard_id] = kinesis.get_shard_iterator(**kwargs)["ShardIterator"]

    por_shard, por_clave = Counter({s: 0 for s in shards}), Counter()
    rondas_vacias = 0
    while iteradores and rondas_vacias < 5:
        hubo = False
        for shard_id, it in list(iteradores.items()):
            salida = kinesis.get_records(ShardIterator=it)
            for r in salida["Records"]:
                hubo = True
                for linea in r["Data"].decode("utf-8").strip().splitlines():
                    por_shard[shard_id] += 1
                    por_clave[json.loads(linea)["contenedor_id"]] += 1
            if salida.get("NextShardIterator"):
                iteradores[shard_id] = salida["NextShardIterator"]
            else:
                del iteradores[shard_id]
        rondas_vacias = 0 if hubo else rondas_vacias + 1
        time.sleep(0.25 if hubo else 2)   # máximo 5 GetRecords/s por shard

    total = sum(por_shard.values())
    print(f"\nTotal: {total} registros en {len(shards)} shards\n")
    barras(por_shard, shards)
    if total:
        clave, n = por_clave.most_common(1)[0]
        print(f"\nClave más frecuente: {clave} con {n} registros ({100 * n / total:.1f} % del tráfico)")
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["shard", "registros"])
            w.writerows((s, por_shard[s]) for s in shards)
        print(f"Guardado en {args.csv}")


if __name__ == "__main__":
    main()
