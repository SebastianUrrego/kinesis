"""Ejercicio 6 (opcional): consumidor enhanced fan-out con SubscribeToShard.

Uso (dos terminales, mismo shard, mientras corre el productor):
    python3 src/consumidor_efo.py --stream ej-compite --nombre efo-1 --duracion 320
    python3 src/consumidor_efo.py --stream ej-compite --nombre efo-2 --duracion 320

Cada consumidor registrado tiene 2 MB/s dedicados por shard y recibe los registros por
push (HTTP/2), así que no compite con los demás. La suscripción dura unos 5 minutos y se
renueva desde el último ContinuationSequenceNumber. Al terminar se elimina el registro del
consumidor, porque cobra por consumidor-shard-hora.
"""
import argparse
import time

import boto3
from botocore.exceptions import ClientError

from consumidor_shard import listar_shards

REGION = "us-east-1"


def registrar(kinesis, stream_arn, nombre):
    try:
        arn = kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName=nombre)["Consumer"]["ConsumerARN"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
        arn = kinesis.describe_stream_consumer(StreamARN=stream_arn, ConsumerName=nombre)[
            "ConsumerDescription"]["ConsumerARN"]
    while True:
        estado = kinesis.describe_stream_consumer(ConsumerARN=arn)["ConsumerDescription"]["ConsumerStatus"]
        print(f"[{nombre}] consumidor {estado}", flush=True)
        if estado == "ACTIVE":
            return arn
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--nombre", default="efo-1", help="ConsumerName registrado en el stream")
    parser.add_argument("--indice-shard", type=int, default=0)
    parser.add_argument("--duracion", type=int, default=0, help="segundos que corre (0 = hasta Ctrl+C)")
    parser.add_argument("--conservar", action="store_true", help="no eliminar el consumidor al terminar")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=REGION)
    stream_arn = kinesis.describe_stream_summary(StreamName=args.stream)["StreamDescriptionSummary"]["StreamARN"]
    shard_id = listar_shards(kinesis, args.stream)[args.indice_shard]["ShardId"]
    arn = registrar(kinesis, stream_arn, args.nombre)

    posicion = {"Type": "LATEST"}
    total = errores = en_segundo = 0
    inicio = marca = time.time()
    primero = ultimo = None
    try:
        while not args.duracion or time.time() - inicio < args.duracion:
            try:
                eventos = kinesis.subscribe_to_shard(ConsumerARN=arn, ShardId=shard_id,
                                                     StartingPosition=posicion)["EventStream"]
                for evento in eventos:
                    datos = evento["SubscribeToShardEvent"]
                    n = sum(len(r["Data"].decode("utf-8").strip().splitlines()) for r in datos["Records"])
                    total += n
                    en_segundo += n
                    if n:
                        primero = primero or time.time()
                        ultimo = time.time()
                    posicion = {"Type": "AFTER_SEQUENCE_NUMBER",
                                "SequenceNumber": datos["ContinuationSequenceNumber"]}
                    if time.time() - marca >= 1:
                        print(f"[{args.nombre}] t={time.time() - inicio:5.0f}s  registros/s: {en_segundo:5d}"
                              f"  | total: {total}, errores: {errores}", flush=True)
                        marca, en_segundo = time.time(), 0
                    if args.duracion and time.time() - inicio >= args.duracion:
                        break
                print(f"[{args.nombre}] suscripción terminada (dura ~5 min); renovando", flush=True)
            except ClientError as e:
                # Solo se admite una suscripción por consumidor y shard cada 5 s.
                errores += 1
                print(f"[{args.nombre}] {e.response['Error']['Code']}; reintento en 5 s", flush=True)
                time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        if not args.conservar:
            kinesis.deregister_stream_consumer(ConsumerARN=arn)
            print(f"[{args.nombre}] consumidor eliminado del stream", flush=True)
    activo = max((ultimo or 0) - (primero or 0), 1)
    print(f"RESUMEN {args.nombre} (EFO): {total} registros, {total / activo:.1f} registros/s, {errores} errores")


if __name__ == "__main__":
    main()
