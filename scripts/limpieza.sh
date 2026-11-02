#!/usr/bin/env bash
# Lab 6 (obligatorio): elimina todos los recursos del taller. Se puede ejecutar varias veces.
#
# Uso: ./scripts/limpieza.sh <bucket>
# Sin argumento, borra todo menos el bucket y muestra los buckets taller-kinesis-* que quedan.
# Los log groups de CloudWatch se conservan como evidencia.
set -uo pipefail

BUCKET=${1:-}
export AWS_DEFAULT_REGION=us-east-1
STREAMS=(taller-kinesis-sensores ej-compite ej-reparto ej-reshard)   # taller + ejercicios adicionales
FIREHOSE=taller-kinesis-firehose
LAMBDAS=(procesar-sensores taller-kinesis-planb-s3 taller-kinesis-contar-alertas)
TABLA_LEASES=leases                                                 # reto de los ejercicios adicionales

# --- Lambdas y sus triggers ------------------------------------------------------------
for f in "${LAMBDAS[@]}"; do
  aws lambda get-function --function-name "$f" >/dev/null 2>&1 || continue
  for uuid in $(aws lambda list-event-source-mappings --function-name "$f" \
      --query 'EventSourceMappings[].UUID' --output text); do
    echo "[lambda] borrando trigger $uuid de $f"
    aws lambda delete-event-source-mapping --uuid "$uuid" >/dev/null
  done
  echo "[lambda] borrando $f"
  aws lambda delete-function --function-name "$f"
done

# --- Firehose ----------------------------------------------------------------------------
if aws firehose describe-delivery-stream --delivery-stream-name "$FIREHOSE" >/dev/null 2>&1; then
  echo "[firehose] borrando $FIREHOSE"
  aws firehose delete-delivery-stream --delivery-stream-name "$FIREHOSE"
fi

# --- Kinesis Data Streams (consumen crédito mientras existan) --------------------------
# --enforce-consumer-deletion también elimina los consumidores enhanced fan-out (Ej. 6).
for s in "${STREAMS[@]}"; do
  if aws kinesis describe-stream-summary --stream-name "$s" >/dev/null 2>&1; then
    echo "[stream] borrando $s"
    aws kinesis delete-stream --stream-name "$s" --enforce-consumer-deletion
    aws kinesis wait stream-not-exists --stream-name "$s"
  fi
done

# --- Tabla DynamoDB de leases (reto de los ejercicios adicionales) ---------------------
if aws dynamodb describe-table --table-name "$TABLA_LEASES" >/dev/null 2>&1; then
  echo "[dynamodb] borrando $TABLA_LEASES"
  aws dynamodb delete-table --table-name "$TABLA_LEASES" >/dev/null
  aws dynamodb wait table-not-exists --table-name "$TABLA_LEASES"
fi

# --- Bucket S3 ---------------------------------------------------------------------------
if [ -n "$BUCKET" ]; then
  if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo "[s3] vaciando y borrando $BUCKET"
    aws s3 rb "s3://$BUCKET" --force
  fi
else
  echo "[s3] sin bucket indicado; buckets del taller que siguen existiendo:"
  aws s3api list-buckets --query "Buckets[?starts_with(Name, 'taller-kinesis-')].Name" --output text
fi

# --- Verificación (evidencia para la rúbrica) ------------------------------------------
echo
echo "== Verificación =="
echo "Streams de Kinesis:  $(aws kinesis list-streams --query 'StreamNames' --output text)"
echo "Streams de Firehose: $(aws firehose list-delivery-streams --query 'DeliveryStreamNames' --output text)"
echo "Lambdas del taller:  $(aws lambda list-functions \
  --query "Functions[?FunctionName=='procesar-sensores' || starts_with(FunctionName, 'taller-kinesis-')].FunctionName" \
  --output text)"
echo "Buckets del taller:  $(aws s3api list-buckets \
  --query "Buckets[?starts_with(Name, 'taller-kinesis-')].Name" --output text)"
echo "Tabla de leases:     $(aws dynamodb list-tables --query "TableNames[?@=='$TABLA_LEASES']" --output text)"
echo "Todo lo anterior debe estar vacío. Después pulse End Lab."
