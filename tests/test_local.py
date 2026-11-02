"""Pruebas locales sin AWS: Lambdas con eventos de Kinesis simulados y reintentos del productor.

Ejecutar desde la raíz del repo:  python -m unittest discover -s tests -v
"""
import base64
import json
import os
import sys
import types
import unittest
from unittest import mock

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(RAIZ, "lambda"), os.path.join(RAIZ, "src")]

# Las Lambdas crean el cliente S3 al importarse; con un boto3 falso no hace falta AWS ni boto3.
boto3_falso = types.ModuleType("boto3")
boto3_falso.client = lambda *a, **k: mock.MagicMock()
sys.modules.setdefault("boto3", boto3_falso)
try:
    from botocore.exceptions import ClientError
except ImportError:  # sin botocore instalado: una ClientError mínima basta para las pruebas
    class ClientError(Exception):
        def __init__(self, response, operation):
            super().__init__(operation)
            self.response = response
    botocore_falso = types.ModuleType("botocore")
    botocore_falso.exceptions = types.SimpleNamespace(ClientError=ClientError)
    sys.modules["botocore"], sys.modules["botocore.exceptions"] = botocore_falso, botocore_falso.exceptions

import consumidor_leases  # noqa: E402
import consumidor_shard  # noqa: E402
import contar_alertas  # noqa: E402
import plan_b_s3_writer  # noqa: E402
import procesar_sensores  # noqa: E402
import productor  # noqa: E402
import productor_param  # noqa: E402


def evento_kinesis(eventos, shard="shardId-000000000000"):
    registros = []
    for i, e in enumerate(eventos):
        seq = f"4960{i:04d}"
        registros.append({
            "eventID": f"{shard}:{seq}",
            "kinesis": {
                "sequenceNumber": seq,
                "data": base64.b64encode((json.dumps(e) + "\n").encode()).decode(),
            },
        })
    return {"Records": registros}


EVENTOS = [
    {"contenedor_id": "CT-001", "temperatura": 9.5, "humedad": 50.0, "timestamp": "2026-10-28T10:00:00+00:00"},
    {"contenedor_id": "CT-001", "temperatura": 4.0, "humedad": 55.0, "timestamp": "2026-10-28T10:00:01+00:00"},
    {"contenedor_id": "CT-002", "temperatura": 12.1, "humedad": 60.0, "timestamp": "2026-10-29T00:00:02+00:00"},
]


class TestProcesarSensores(unittest.TestCase):
    def test_alertas_y_sin_fallos(self):
        with mock.patch("builtins.print") as p:
            res = procesar_sensores.lambda_handler(evento_kinesis(EVENTOS), None)
        salida = [c.args[0] for c in p.call_args_list]
        self.assertEqual(res, {"batchItemFailures": []})
        self.assertEqual(sum(s.startswith("ALERTA") for s in salida), 2)
        self.assertEqual(sum(s.startswith("OK") for s in salida), 1)

    def test_registro_corrupto_reporta_fallo(self):
        ev = evento_kinesis(EVENTOS)
        ev["Records"][1]["kinesis"]["data"] = base64.b64encode(b"no es json").decode()
        with mock.patch("builtins.print"):
            res = procesar_sensores.lambda_handler(ev, None)
        self.assertEqual(res["batchItemFailures"], [{"itemIdentifier": "49600001"}])


class TestPlanB(unittest.TestCase):
    def test_particiona_por_dia_con_clave_determinista(self):
        s3 = mock.MagicMock()
        with mock.patch.object(plan_b_s3_writer, "s3", s3), \
                mock.patch.dict(os.environ, {"BUCKET": "b"}), mock.patch("builtins.print"):
            plan_b_s3_writer.lambda_handler(evento_kinesis(EVENTOS), None)
            plan_b_s3_writer.lambda_handler(evento_kinesis(EVENTOS), None)  # reintento del mismo lote
        claves = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
        self.assertEqual(sorted(set(claves)), [
            "sensores/anio=2026/mes=10/dia=28/shardId-000000000000-49600000-49600002.json",
            "sensores/anio=2026/mes=10/dia=29/shardId-000000000000-49600000-49600002.json",
        ])
        self.assertEqual(len(claves), 4)  # el reintento sobrescribe las mismas 2 claves
        cuerpo = s3.put_object.call_args_list[0].kwargs["Body"].decode()
        self.assertEqual(len(cuerpo.strip().splitlines()), 2)


class TestContarAlertas(unittest.TestCase):
    def test_resumen_por_contenedor(self):
        s3 = mock.MagicMock()
        with mock.patch.object(contar_alertas, "s3", s3), \
                mock.patch.dict(os.environ, {"BUCKET": "b"}), mock.patch("builtins.print"):
            contar_alertas.lambda_handler(evento_kinesis(EVENTOS), None)
        kwargs = s3.put_object.call_args.kwargs
        self.assertTrue(kwargs["Key"].startswith("resumen-alertas/anio="))
        filas = {f["contenedor_id"]: f for f in map(json.loads, kwargs["Body"].decode().splitlines())}
        self.assertEqual((filas["CT-001"]["eventos"], filas["CT-001"]["alertas"]), (2, 1))
        self.assertEqual((filas["CT-002"]["eventos"], filas["CT-002"]["alertas"]), (1, 1))
        self.assertEqual(filas["CT-001"]["temp_max"], 9.5)


class TestProductor(unittest.TestCase):
    def test_reintenta_solo_los_fallidos(self):
        kinesis = mock.MagicMock()
        kinesis.put_records.side_effect = [
            {"FailedRecordCount": 1, "Records": [
                {"SequenceNumber": "1"}, {"ErrorCode": "ProvisionedThroughputExceededException"}]},
            {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "2"}]},
        ]
        lote = [productor.a_registro(productor.generar_evento(), False) for _ in range(2)]
        with mock.patch("time.sleep"), mock.patch("builtins.print"):
            enviados, perdidos = productor.enviar_lote(kinesis, "s", lote)
        self.assertEqual((enviados, perdidos), (2, 0))
        self.assertEqual(kinesis.put_records.call_args_list[1].kwargs["Records"], [lote[1]])

    def test_formato_del_registro(self):
        e = productor.generar_evento()
        r = productor.a_registro(e, key_constante=False)
        self.assertEqual(r["PartitionKey"], e["contenedor_id"])
        self.assertTrue(r["Data"].endswith(b"\n"))
        self.assertEqual(set(json.loads(r["Data"])), {"contenedor_id", "temperatura", "humedad", "timestamp"})
        self.assertEqual(productor.a_registro(e, key_constante=True)["PartitionKey"], "constante")


class TestProductorParam(unittest.TestCase):
    def test_sesgo_y_claves(self):
        lista = productor_param.claves(30)
        self.assertEqual((len(lista), lista[0], lista[-1]), (30, "CT-001", "CT-030"))
        self.assertEqual({productor_param.elegir_clave(lista, 1.0) for _ in range(50)}, {"CT-001"})
        self.assertGreater(len({productor_param.elegir_clave(lista, 0.0) for _ in range(500)}), 20)

    def test_fallo_parcial_reenvia_solo_los_fallidos(self):
        kinesis = mock.MagicMock()
        kinesis.put_records.side_effect = [
            {"FailedRecordCount": 1, "Records": [
                {"SequenceNumber": "1"}, {"ErrorCode": "ProvisionedThroughputExceededException"}]},
            {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "2"}]},
        ]
        lote = [productor_param.generar_registro(["CT-001"], 0) for _ in range(2)]
        with mock.patch("time.sleep"):
            self.assertEqual(productor_param.enviar_lote(kinesis, "s", lote), (2, 1, 0))
        self.assertEqual(kinesis.put_records.call_args_list[1].kwargs["Records"], [lote[1]])


class TestConsumidorShard(unittest.TestCase):
    SHARDS = [
        {"ShardId": "shardId-000000000000", "SequenceNumberRange": {"EndingSequenceNumber": "9"}},
        {"ShardId": "shardId-000000000002", "ParentShardId": "shardId-000000000000"},
        {"ShardId": "shardId-000000000003", "ParentShardId": "shardId-000000000000"},
        {"ShardId": "shardId-000000000004", "ParentShardId": "shardId-000000000001",
         "AdjacentParentShardId": "shardId-000000000002"},
    ]

    def test_hijos_de_un_split_y_de_un_merge(self):
        self.assertEqual(consumidor_shard.hijos(self.SHARDS, "shardId-000000000000"),
                         ["shardId-000000000002", "shardId-000000000003"])
        self.assertEqual(consumidor_shard.hijos(self.SHARDS, "shardId-000000000002"), ["shardId-000000000004"])
        self.assertEqual(consumidor_shard.padres(self.SHARDS[3]), ["shardId-000000000001", "shardId-000000000002"])


class TestLeases(unittest.TestCase):
    def test_objetivo_reparte_parejo(self):
        self.assertEqual([consumidor_leases.objetivo(4, w) for w in (0, 1, 2, 3)], [4, 4, 2, 2])

    def test_tomar_lease_ocupado_pierde_la_escritura_condicional(self):
        c = object.__new__(consumidor_leases.Consumidor)
        c.args = types.SimpleNamespace(ttl=15, stream="s")
        c.yo, c.mios = "w2", {}
        c.tabla = mock.MagicMock()
        c.tabla.update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
        self.assertFalse(c.tomar("shardId-000000000000"))
        condicion = c.tabla.update_item.call_args.kwargs["ConditionExpression"]
        self.assertIn("attribute_not_exists(shard_id)", condicion)
        self.assertEqual(c.mios, {})


if __name__ == "__main__":
    unittest.main()
