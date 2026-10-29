-- Lab 5 (opcional) y reto final: consultas en Athena sobre el data lake.
-- Antes de ejecutar:
--   1. Reemplace <su-bucket> por el nombre de su bucket.
--   2. En Athena → Settings, configure la ubicación de resultados:
--      s3://<su-bucket>/athena-resultados/
-- Athena lee recursivamente las subcarpetas anio=/mes=/dia= bajo LOCATION.

-- 1. Eventos crudos (escritos por Firehose o por el Plan B; una línea JSON por evento)
CREATE EXTERNAL TABLE IF NOT EXISTS sensores (
  contenedor_id string,
  temperatura double,
  humedad double,
  `timestamp` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<su-bucket>/sensores/';

-- Temperatura promedio por contenedor (consulta del enunciado)
SELECT contenedor_id, avg(temperatura) AS temp_prom, count(*) AS n
FROM sensores
GROUP BY contenedor_id
ORDER BY temp_prom DESC;

-- Alertas (> 8 °C) por contenedor a partir de los eventos crudos
SELECT contenedor_id,
       count_if(temperatura > 8.0) AS alertas,
       count(*) AS eventos,
       round(100.0 * count_if(temperatura > 8.0) / count(*), 1) AS pct_alertas,
       max(temperatura) AS temp_max
FROM sensores
GROUP BY contenedor_id
ORDER BY alertas DESC;

-- 2. Reto: resúmenes parciales escritos por la Lambda contar_alertas
CREATE EXTERNAL TABLE IF NOT EXISTS resumen_alertas (
  contenedor_id string,
  eventos int,
  alertas int,
  temp_max double,
  umbral double,
  procesado string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<su-bucket>/resumen-alertas/';

-- Total por contenedor sumando los resúmenes de cada lote.
-- Debe coincidir con la consulta de alertas sobre la tabla sensores.
SELECT contenedor_id,
       sum(alertas) AS alertas,
       sum(eventos) AS eventos,
       max(temp_max) AS temp_max
FROM resumen_alertas
GROUP BY contenedor_id
ORDER BY alertas DESC;
