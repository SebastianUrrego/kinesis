"""Ejercicios 2, 3 y 5: consumidor de UN shard con GetRecords.

Uso:
    # Ej. 2: tres consumidores sobre el mismo shard (compiten por la lectura)
    python3 src/consumidor_shard.py --stream ej-compite --indice-shard 0 --pausa 0.4 --nombre c1 --duracion 320
    # Ej. 3: un consumidor por shard
    python3 src/consumidor_shard.py --stream ej-reparto --indice-shard 1 --pausa 0.4 --nombre c2
    # Ej. 5: al agotar un shard cerrado, continúa con sus hijos y verifica el orden por clave
    python3 src/consumidor_shard.py --stream ej-reshard --indice-shard 0 --desde TRIM_HORIZON --seguir-hijos --verificar

Imprime cada segundo los registros recibidos y los ProvisionedThroughputExceededException,
y al terminar (--duracion o Ctrl+C) una línea RESUMEN para la tabla del entregable.
"""
import argparse
import json
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"


def listar_shards(kinesis, stream):
    """Shards ordenados por ShardId, con ParentShardId y EndingSequenceNumber si los tienen."""
    shards, token = [], None
    while True:
        salida = kinesis.list_shards(NextToken=token) if token else kinesis.list_shards(StreamName=stream)
        shards += salida["Shards"]
        token = salida.get("NextToken")
        if not token:
            return sorted(shards, key=lambda s: s["ShardId"])


def hijos(shards, padre_id):
    return [s["ShardId"] for s in shards
            if padre_id in (s.get("ParentShardId"), s.get("AdjacentParentShardId"))]


def padres(shard):
    return [p for p in (shard.get("ParentShardId"), shard.get("AdjacentParentShardId")) if p]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--indice-shard", type=int, default=0, help="posición del shard en list_shards (0, 1, 2…)")
    parser.add_argument("--pausa", type=float, default=1.0, help="segundos entre llamadas GetRecords")
    parser.add_argument("--nombre", default="c1")
    parser.add_argument("--desde", default="LATEST", choices=["LATEST", "TRIM_HORIZON"])
    parser.add_argument("--duracion", type=int, default=0, help="segundos que corre (0 = hasta Ctrl+C)")
    parser.add_argument("--seguir-hijos", action="store_true", help="Ej. 5: al cerrarse el shard, leer sus hijos")
    parser.add_argument("--verificar", action="store_true",
                        help="Ej. 5: comprueba que el timestamp de cada clave nunca retrocede")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    shards = listar_shards(kinesis, args.stream)
    shard_inicial = shards[args.indice_shard]["ShardId"]
    print(f"[{args.nombre}] shards del stream: {[s['ShardId'] for s in shards]}; leo {shard_inicial}", flush=True)

    def abrir(shard_id, tipo):
        print(f"[{args.nombre}] leyendo {shard_id}", flush=True)
        return kinesis.get_shard_iterator(StreamName=args.stream, ShardId=shard_id,
                                          ShardIteratorType=tipo)["ShardIterator"]

    # Normalmente hay un solo shard activo; tras un split hay que leer los dos hijos a la vez.
    activos = {shard_inicial: abrir(shard_inicial, args.desde)}
    terminados = set()
    total = errores = en_segundo = err_segundo = inversiones = 0
    ultimo_ts = {}
    inicio = marca = time.time()
    primero = ultimo = None
    try:
        while activos and not (args.duracion and time.time() - inicio >= args.duracion):
            for shard_id, iterador in list(activos.items()):
                try:
                    salida = kinesis.get_records(ShardIterator=iterador)
                except ClientError as e:
                    if e.response["Error"]["Code"] != "ProvisionedThroughputExceededException":
                        raise
                    # Límite de 5 GetRecords/s o 2 MB/s por shard, compartido entre consumidores.
                    errores += 1
                    err_segundo += 1
                    time.sleep(0.5)
                    continue
                for r in salida["Records"]:
                    for linea in r["Data"].decode("utf-8").strip().splitlines():
                        total += 1
                        en_segundo += 1
                        if args.verificar:
                            e = json.loads(linea)
                            if e["timestamp"] < ultimo_ts.get(e["contenedor_id"], ""):
                                inversiones += 1
                            ultimo_ts[e["contenedor_id"]] = e["timestamp"]
                if salida["Records"]:
                    primero = primero or time.time()
                    ultimo = time.time()
                if salida.get("NextShardIterator"):
                    activos[shard_id] = salida["NextShardIterator"]
                    continue
                # Sin NextShardIterator el shard está cerrado y ya se leyó entero.
                del activos[shard_id]
                terminados.add(shard_id)
                print(f"[{args.nombre}] {shard_id} cerrado y leído completo ({total} registros hasta ahora)",
                      flush=True)
                if args.seguir_hijos:
                    # Volver a listar: los hijos pueden haberse creado después de arrancar.
                    shards = listar_shards(kinesis, args.stream)
                    por_id = {s["ShardId"]: s for s in shards}
                    for h in hijos(shards, shard_id):
                        # Un hijo de un merge tiene dos padres: se lee solo cuando ambos terminaron.
                        # Se lee desde su inicio para no perder lo que llegó tras el reshard.
                        if h not in activos and all(p in terminados for p in padres(por_id[h])):
                            activos[h] = abrir(h, "TRIM_HORIZON")
            if time.time() - marca >= 1:
                print(f"[{args.nombre}] t={time.time() - inicio:5.0f}s  registros/s: {en_segundo:5d}"
                      f"  errores/s: {err_segundo:2d}  | total: {total}, errores: {errores}", flush=True)
                marca, en_segundo, err_segundo = time.time(), 0, 0
            time.sleep(args.pausa)
    except KeyboardInterrupt:
        pass
    # Tasa medida mientras llegaban datos, para no diluirla con la espera antes del productor.
    activo = max((ultimo or 0) - (primero or 0), 1)
    print(f"RESUMEN {args.nombre}: shard inicial {shard_inicial}, "
          f"{total} registros, {total / activo:.1f} registros/s, {errores} errores de límite"
          + (f", {inversiones} inversiones de orden por clave" if args.verificar else ""), flush=True)


if __name__ == "__main__":
    main()
