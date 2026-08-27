-- =============================================================================
-- Database layout
--
--   raw                -> CDC landing zone. Written only by Kafka-engine
--                         materialized views. Nothing here is hand-curated.
--   analytics_staging  -> dbt staging models (cleaned, deduplicated, typed).
--   analytics_marts    -> dbt marts (analytics- and ML-ready).
--   analytics_ops      -> pipeline metadata: DQ results, run audit.
--
-- Separating them by database (rather than by prefix in one database) means
-- grants, TTLs and backup policy can differ per layer, and a `SHOW TABLES`
-- in any one layer is legible on its own.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics_staging;
CREATE DATABASE IF NOT EXISTS analytics_marts;
CREATE DATABASE IF NOT EXISTS analytics_ops;
