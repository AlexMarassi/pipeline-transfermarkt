-- Vista sobre el Gold particionado. {{GOLD_GLOB}} lo reemplaza la tarea update_catalog.
-- hive_partitioning expone year y month (de las carpetas year=YYYY/month=MM) como columnas,
-- y DuckDB las usa para leer solo las carpetas que pide el WHERE (partition pruning).
CREATE OR REPLACE VIEW player_appearances AS
SELECT *
FROM read_parquet('{{GOLD_GLOB}}', hive_partitioning = true);
