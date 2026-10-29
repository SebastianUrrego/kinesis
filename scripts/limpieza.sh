#!/usr/bin/env bash
# Lab 6 (obligatorio): elimina todos los recursos del taller. Se puede ejecutar varias veces.
#
# Uso: ./scripts/limpieza.sh <bucket>
# Sin argumento, borra todo menos el bucket y muestra los buckets taller-kinesis-* que quedan.
# Los log groups de CloudWatch se conservan como evidencia.
set -uo pipefail

BUCKET=${1:-}
export AWS_DEFAULT_REGION=us-east-1
STREAM=taller-kinesis-sensores
FIREHOSE=taller-kinesis-firehose
LAMBDAS=(procesar-sensores taller-kinesis-planb-s3 taller-kinesis-contar-alertas)

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

# --- Kinesis Data Stream (consume crédito mientras exista) -----------------------------
if aws kinesis describe-stream-summary --stream-name "$STREAM" >/dev/null 2>&1; then
  echo "[stream] borrando $STREAM"
  aws kinesis delete-stream --stream-name "$STREAM" --enforce-consumer-deletion
  aws kinesis wait stream-not-exists --stream-name "$STREAM"
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
echo "Todo lo anterior debe estar vacío. Después pulse End Lab."
