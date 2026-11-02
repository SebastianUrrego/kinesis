"""Ejercicio 1: productor parametrizable (tasa, número de claves y sesgo).

Uso:
    python3 src/productor_param.py --stream ej-compite --tasa 200 --duracion 300 --claves 8
    python3 src/productor_param.py --stream ej-reparto --tasa 300 --duracion 120 --claves 30
    python3 src/productor_param.py --stream ej-reparto --tasa 300 --duracion 60 --claves 30 --sesgo 0.8

Cada segundo genera --tasa eventos, los envía con put_records en lotes de hasta 500 y
reenvía solo los registros fallidos (FailedRecordCount > 0) con backoff exponencial.
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
MAX_LOTE = 500          # límite de put_records por llamada
MAX_REINTENTOS = 6
CLAVE_CALIENTE = "CT-001"


def claves(n):
    return [f"CT-{i:03d}" for i in range(1, n + 1)]


def elegir_clave(lista, sesgo):
    # Con probabilidad --sesgo, siempre CT-001; si no, una clave al azar (CT-001 incluida).
    return CLAVE_CALIENTE if random.random() < sesgo else random.choice(lista)


def generar_registro(lista, sesgo):
    evento = {
        "contenedor_id": elegir_clave(lista, sesgo),
        "temperatura": round(random.gauss(5, 3), 2),
        "humedad": round(random.uniform(40, 90), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return {"Data": (json.dumps(evento) + "\n").encode("utf-8"), "PartitionKey": evento["contenedor_id"]}


def enviar_lote(kinesis, stream, registros):
    """put_records con reintento solo de los fallidos. Devuelve (enviados, reintentados, fallidos).

    La llamada no lanza excepción cuando fallan algunos registros: hay que mirar
    FailedRecordCount y el ErrorCode de cada posición de la respuesta.
    """
    pendientes, enviados, reintentados = registros, 0, 0
    for intento in range(MAX_REINTENTOS):
        respuesta = kinesis.put_records(StreamName=stream, Records=pendientes)
        if respuesta["FailedRecordCount"] == 0:
            return enviados + len(pendientes), reintentados, 0
        fallidos = [reg for reg, res in zip(pendientes, respuesta["Records"]) if "ErrorCode" in res]
        enviados += len(pendientes) - len(fallidos)
        reintentados += len(fallidos)
        time.sleep(min(2 ** intento * 0.1, 3) * random.uniform(0.5, 1.0))   # backoff con jitter
        pendientes = fallidos
    return enviados, reintentados, len(pendientes)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--tasa", type=int, default=100, help="registros por segundo")
    parser.add_argument("--duracion", type=int, default=60, help="segundos que corre")
    parser.add_argument("--claves", type=int, default=8, help="número de partition keys distintas")
    parser.add_argument("--sesgo", type=float, default=0.0, help="probabilidad (0 a 1) de usar siempre CT-001")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    lista = claves(args.claves)
    total = {"enviados": 0, "reintentados": 0, "fallidos": 0}
    inicio = time.time()
    for segundo in range(1, args.duracion + 1):
        lote = [generar_registro(lista, args.sesgo) for _ in range(args.tasa)]
        este = [0, 0, 0]
        for i in range(0, len(lote), MAX_LOTE):
            for k, v in enumerate(enviar_lote(kinesis, args.stream, lote[i:i + MAX_LOTE])):
                este[k] += v
        for k, v in zip(total, este):
            total[k] += v
        print(f"t={segundo:4d}s  enviados: {este[0]:5d}  reintentados: {este[1]:4d}  fallidos: {este[2]:4d}"
              f"  | acumulado: {total['enviados']} enviados, {total['fallidos']} fallidos", flush=True)
        # Mantener la tasa: esperar hasta el inicio del siguiente segundo.
        time.sleep(max(0.0, inicio + segundo - time.time()))
    print(f"RESUMEN productor: {total['enviados']} enviados, {total['reintentados']} reintentados, "
          f"{total['fallidos']} fallidos en {args.duracion} s ({args.claves} claves, sesgo {args.sesgo})")


if __name__ == "__main__":
    main()
