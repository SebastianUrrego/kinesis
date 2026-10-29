#!/usr/bin/env bash
# Crea (o actualiza) los recursos del taller con la CLI. Se puede ejecutar varias veces.
#
# Uso (desde CloudShell, en la raíz del repo):
#   ./scripts/crear_recursos.sh <bucket>                 # stream + Lambda alertas + Firehose → S3
#   ./scripts/crear_recursos.sh <bucket> planb           # igual, pero Plan B (Lambda → S3) en vez de Firehose
#   SHARDS=3 RETO=1 ./scripts/crear_recursos.sh <bucket> # reto: 3 shards + Lambda contar_alertas
#
# Todos los servicios usan el rol existente LabRole (el Learner Lab no permite crear roles).
set -euo pipefail

BUCKET=${1:?"Uso: $0 <bucket> [firehose|planb]   (bucket: taller-kinesis-<iniciales>-<número>)"}
MODO=${2:-firehose}
SHARDS=${SHARDS:-1}
RETO=${RETO:-0}

export AWS_DEFAULT_REGION=us-east-1
STREAM=taller-kinesis-sensores
FIREHOSE=taller-kinesis-firehose
RAIZ=$(cd "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/LabRole"
echo "Cuenta: $ACCOUNT | Región: $AWS_DEFAULT_REGION | Rol: $ROLE_ARN"

# --- 1. Kinesis Data Stream (provisionado) -------------------------------------------
if aws kinesis describe-stream-summary --stream-name "$STREAM" >/dev/null 2>&1; then
  echo "[stream] $STREAM ya existe"
else
  echo "[stream] creando $STREAM con $SHARDS shard(s)"
  aws kinesis create-stream --stream-name "$STREAM" --shard-count "$SHARDS" \
    --stream-mode-details StreamMode=PROVISIONED
fi
aws kinesis wait stream-exists --stream-name "$STREAM"

ABIERTOS=$(aws kinesis describe-stream-summary --stream-name "$STREAM" \
  --query StreamDescriptionSummary.OpenShardCount --output text)
if [ "$ABIERTOS" != "$SHARDS" ]; then
  echo "[stream] resharding $ABIERTOS → $SHARDS shards"
  aws kinesis update-shard-count --stream-name "$STREAM" --target-shard-count "$SHARDS" \
    --scaling-type UNIFORM_SCALING >/dev/null
  aws kinesis wait stream-exists --stream-name "$STREAM"
  until [ "$(aws kinesis describe-stream-summary --stream-name "$STREAM" \
      --query StreamDescriptionSummary.StreamStatus --output text)" = "ACTIVE" ]; do
    sleep 5
  done
fi
STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name "$STREAM" \
  --query StreamDescriptionSummary.StreamARN --output text)

# --- 2. Bucket S3 ----------------------------------------------------------------------
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "[s3] $BUCKET ya existe"
else
  echo "[s3] creando $BUCKET"
  aws s3api create-bucket --bucket "$BUCKET" >/dev/null   # us-east-1 no lleva LocationConstraint
fi

# --- 3. Lambdas con trigger de Kinesis -------------------------------------------------
# crear_lambda <nombre> <módulo> <batch> <ventana_s> <LATEST|TRIM_HORIZON> [Variables={...}]
crear_lambda() {
  local nombre=$1 modulo=$2 batch=$3 ventana=$4 inicio=$5 entorno=${6:-}
  local zip="$TMP/$modulo.zip"
  python3 -m zipfile -c "$zip" "$RAIZ/lambda/$modulo.py"

  if aws lambda get-function --function-name "$nombre" >/dev/null 2>&1; then
    echo "[lambda] actualizando código de $nombre"
    aws lambda update-function-code --function-name "$nombre" --zip-file "fileb://$zip" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$nombre"
  else
    echo "[lambda] creando $nombre"
    local extra=()
    [ -n "$entorno" ] && extra=(--environment "$entorno")
    aws lambda create-function --function-name "$nombre" --runtime python3.12 \
      --role "$ROLE_ARN" --handler "$modulo.lambda_handler" --timeout 60 \
      --zip-file "fileb://$zip" "${extra[@]}" >/dev/null
    aws lambda wait function-active-v2 --function-name "$nombre"
  fi

  local existe
  existe=$(aws lambda list-event-source-mappings --function-name "$nombre" \
    --event-source-arn "$STREAM_ARN" --query 'length(EventSourceMappings)' --output text)
  if [ "$existe" = "0" ]; then
    echo "[lambda] trigger Kinesis → $nombre (batch $batch, ventana ${ventana}s, $inicio)"
    aws lambda create-event-source-mapping --function-name "$nombre" \
      --event-source-arn "$STREAM_ARN" --batch-size "$batch" \
      --maximum-batching-window-in-seconds "$ventana" --starting-position "$inicio" \
      --function-response-types ReportBatchItemFailures >/dev/null
  else
    echo "[lambda] trigger de $nombre ya existe"
  fi
}

crear_lambda procesar-sensores procesar_sensores 100 5 LATEST

# --- 4. Persistencia en S3: Firehose o Plan B ------------------------------------------
if [ "$MODO" = "planb" ]; then
  crear_lambda taller-kinesis-planb-s3 plan_b_s3_writer 100 60 TRIM_HORIZON "Variables={BUCKET=$BUCKET}"
elif aws firehose describe-delivery-stream --delivery-stream-name "$FIREHOSE" >/dev/null 2>&1; then
  echo "[firehose] $FIREHOSE ya existe"
else
  echo "[firehose] creando $FIREHOSE → s3://$BUCKET/sensores/"
  cat > "$TMP/s3.json" <<EOF
{
  "RoleARN": "$ROLE_ARN",
  "BucketARN": "arn:aws:s3:::$BUCKET",
  "Prefix": "sensores/anio=!{timestamp:yyyy}/mes=!{timestamp:MM}/dia=!{timestamp:dd}/",
  "ErrorOutputPrefix": "errores/!{firehose:error-output-type}/anio=!{timestamp:yyyy}/mes=!{timestamp:MM}/dia=!{timestamp:dd}/",
  "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
  "CompressionFormat": "UNCOMPRESSED"
}
EOF
  if ! aws firehose create-delivery-stream --delivery-stream-name "$FIREHOSE" \
      --delivery-stream-type KinesisStreamAsSource \
      --kinesis-stream-source-configuration "KinesisStreamARN=$STREAM_ARN,RoleARN=$ROLE_ARN" \
      --extended-s3-destination-configuration "file://$TMP/s3.json" >/dev/null; then
    echo "[firehose] no se pudo crear (¿Firehose o LabRole no permitidos?). Use el Plan B:"
    echo "  $0 $BUCKET planb"
    exit 1
  fi
fi

# --- 5. Reto: Lambda que cuenta alertas por contenedor ---------------------------------
if [ "$RETO" = "1" ]; then
  crear_lambda taller-kinesis-contar-alertas contar_alertas 500 60 TRIM_HORIZON "Variables={BUCKET=$BUCKET}"
fi

echo
echo "Listo. Siguiente paso: python3 src/productor.py"
echo "Al terminar: ./scripts/limpieza.sh $BUCKET"
