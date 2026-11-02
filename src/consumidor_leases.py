"""Reto: grupo de consumidores con leases en DynamoDB (una versión mínima de KCL).

Uso (un proceso por terminal; mismo --stream y --tabla, distinto --nombre):
    python3 src/consumidor_leases.py --stream ej-reshard --nombre w1
    python3 src/consumidor_leases.py --stream ej-reshard --nombre w2

Tabla DynamoDB (se crea si no existe), clave shard_id:
    shard_id = "shardId-…"   propietario, expira_en (epoch ms), checkpoint (último SequenceNumber)
    shard_id = "worker#w1"   expira_en: latido del consumidor, para saber cuántos hay vivos

Cada consumidor toma como mucho ceil(shards abiertos / consumidores vivos) leases con una
escritura condicional (solo gana uno), renueva sus leases cada pocos segundos, guarda el
checkpoint cada --checkpoint-cada registros y suelta los que le sobran para que otro los tome.
Si un proceso muere, sus leases vencen tras --ttl segundos y otro continúa desde el checkpoint.
"""
import argparse
import math
import os
import socket
import time
from collections import Counter

import boto3
from botocore.exceptions import ClientError

from consumidor_shard import listar_shards

REGION = "us-east-1"
PREFIJO_WORKER = "worker#"


def ahora_ms():
    return int(time.time() * 1000)


def objetivo(n_shards, n_workers):
    """Leases que le tocan a cada consumidor para repartir parejo."""
    return math.ceil(n_shards / max(n_workers, 1))


def es_condicional(e):
    return e.response["Error"]["Code"] == "ConditionalCheckFailedException"


def crear_tabla(dynamodb, nombre):
    try:
        dynamodb.create_table(TableName=nombre, BillingMode="PAY_PER_REQUEST",
                              AttributeDefinitions=[{"AttributeName": "shard_id", "AttributeType": "S"}],
                              KeySchema=[{"AttributeName": "shard_id", "KeyType": "HASH"}])
        print(f"Creando tabla {nombre}…", flush=True)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    dynamodb.get_waiter("table_exists").wait(TableName=nombre)


class Consumidor:
    def __init__(self, args):
        self.args = args
        self.yo = args.nombre
        self.kinesis = boto3.client("kinesis", region_name=REGION)
        crear_tabla(boto3.client("dynamodb", region_name=REGION), args.tabla)
        self.tabla = boto3.resource("dynamodb", region_name=REGION).Table(args.tabla)
        self.mios = {}        # shard_id -> {"iterador", "ultimo_seq", "desde_checkpoint"}
        self.procesados = Counter()

    def log(self, texto):
        print(f"[{self.yo}] {texto}", flush=True)

    # --- leases ----------------------------------------------------------------------------
    def latido(self):
        self.tabla.put_item(Item={"shard_id": PREFIJO_WORKER + self.yo, "expira_en": ahora_ms() + self.args.ttl * 1000})

    def estado(self):
        items = self.tabla.scan(ConsistentRead=True)["Items"]
        vivos = [i for i in items if i["shard_id"].startswith(PREFIJO_WORKER) and i["expira_en"] > ahora_ms()]
        leases = {i["shard_id"]: i for i in items if not i["shard_id"].startswith(PREFIJO_WORKER)}
        return len(vivos), leases

    def tomar(self, shard_id):
        """Escritura condicional: gana solo si el lease no existe, venció o ya es mío."""
        try:
            item = self.tabla.update_item(
                Key={"shard_id": shard_id},
                UpdateExpression="SET propietario = :yo, expira_en = :exp",
                ConditionExpression="attribute_not_exists(shard_id) OR expira_en < :ahora OR propietario = :yo",
                ExpressionAttributeValues={":yo": self.yo, ":exp": ahora_ms() + self.args.ttl * 1000,
                                           ":ahora": ahora_ms()},
                ReturnValues="ALL_NEW")["Attributes"]
        except ClientError as e:
            if es_condicional(e):
                return False
            raise
        checkpoint = item.get("checkpoint")
        if shard_id in self.mios:   # ya era mío: conservo el iterador
            return True
        kwargs = {"StreamName": self.args.stream, "ShardId": shard_id}
        if checkpoint:
            kwargs.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=checkpoint)
        else:
            kwargs.update(ShardIteratorType="TRIM_HORIZON")
        iterador = self.kinesis.get_shard_iterator(**kwargs)["ShardIterator"]
        self.mios[shard_id] = {"iterador": iterador, "ultimo_seq": checkpoint, "sin_guardar": 0}
        self.log(f"tomé {shard_id}" + (f" desde el checkpoint …{checkpoint[-8:]}" if checkpoint else " desde TRIM_HORIZON"))
        return True

    def condicion_mia(self, shard_id, update, valores, nombres=None):
        """Actualiza el lease solo si sigue siendo mío; si no, lo doy por perdido."""
        extra = {"ExpressionAttributeNames": nombres} if nombres else {}
        try:
            self.tabla.update_item(Key={"shard_id": shard_id}, UpdateExpression=update,
                                   ConditionExpression="propietario = :yo",
                                   ExpressionAttributeValues={":yo": self.yo, **valores}, **extra)
            return True
        except ClientError as e:
            if not es_condicional(e):
                raise
            self.log(f"perdí {shard_id} (otro consumidor lo tomó)")
            self.mios.pop(shard_id, None)
            return False

    def renovar(self):
        for shard_id in list(self.mios):
            self.condicion_mia(shard_id, "SET expira_en = :exp", {":exp": ahora_ms() + self.args.ttl * 1000})

    def checkpoint(self, shard_id):
        info = self.mios.get(shard_id)
        if info and info["sin_guardar"]:
            if self.condicion_mia(shard_id, "SET #c = :seq", {":seq": info["ultimo_seq"]}, {"#c": "checkpoint"}):
                info["sin_guardar"] = 0

    def soltar(self, shard_id):
        self.checkpoint(shard_id)
        if self.condicion_mia(shard_id, "SET expira_en = :cero", {":cero": 0}):
            self.mios.pop(shard_id, None)
            self.log(f"solté {shard_id} para equilibrar")

    def equilibrar(self):
        abiertos = [s["ShardId"] for s in listar_shards(self.kinesis, self.args.stream)
                    if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
        vivos, leases = self.estado()
        meta = objetivo(len(abiertos), vivos)
        for shard_id in sorted(self.mios)[meta:]:
            self.soltar(shard_id)
        for shard_id in abiertos:
            if len(self.mios) >= meta:
                break
            lease = leases.get(shard_id)
            libre = lease is None or lease["expira_en"] < ahora_ms() or lease["propietario"] == self.yo
            if shard_id not in self.mios and libre:
                self.tomar(shard_id)
        return vivos, meta

    # --- lectura ---------------------------------------------------------------------------
    def leer(self):
        for shard_id, info in list(self.mios.items()):
            try:
                salida = self.kinesis.get_records(ShardIterator=info["iterador"], Limit=500)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ProvisionedThroughputExceededException":
                    continue
                raise
            for r in salida["Records"]:
                # Aquí iría el procesamiento real; debe ser idempotente (entrega al menos una vez).
                self.procesados[shard_id] += 1
                info["ultimo_seq"] = r["SequenceNumber"]
                info["sin_guardar"] += 1
                if info["sin_guardar"] >= self.args.checkpoint_cada:
                    self.checkpoint(shard_id)
            info["iterador"] = salida.get("NextShardIterator")
            if not info["iterador"]:
                self.checkpoint(shard_id)
                self.mios.pop(shard_id, None)

    def correr(self):
        inicio = time.time()
        ultimo_equilibrio = ultima_renovacion = 0
        try:
            while not self.args.duracion or time.time() - inicio < self.args.duracion:
                if time.time() - ultima_renovacion >= self.args.ttl / 3:
                    self.latido()
                    self.renovar()
                    ultima_renovacion = time.time()
                if time.time() - ultimo_equilibrio >= 5:
                    vivos, meta = self.equilibrar()
                    ultimo_equilibrio = time.time()
                    self.log(f"vivos: {vivos}, objetivo: {meta}, mis shards: {sorted(self.mios) or '-'}, "
                             f"procesados: {dict(self.procesados) or 0}")
                self.leer()
                time.sleep(1)
        except KeyboardInterrupt:
            self.log("Ctrl+C: guardo checkpoints y suelto mis leases")
        for shard_id in list(self.mios):
            self.soltar(shard_id)
        self.tabla.delete_item(Key={"shard_id": PREFIJO_WORKER + self.yo})
        self.log(f"RESUMEN: {sum(self.procesados.values())} registros procesados {dict(self.procesados)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--tabla", default="leases", help="tabla DynamoDB de leases")
    parser.add_argument("--nombre", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--ttl", type=int, default=15, help="segundos que dura un lease sin renovar")
    parser.add_argument("--checkpoint-cada", type=int, default=100, help="registros entre checkpoints")
    parser.add_argument("--duracion", type=int, default=0, help="segundos que corre (0 = hasta Ctrl+C)")
    Consumidor(parser.parse_args()).correr()


if __name__ == "__main__":
    main()
