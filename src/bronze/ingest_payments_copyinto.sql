-- TODO STUB — Bronze: ingest `payments` (Parquet) with COPY INTO.
--
-- Goal: incrementally load the payments Parquet files from the landing Volume into a
-- bronze Delta table using COPY INTO (idempotent, file-level — re-runs skip loaded files).
--
-- Acceptance:
--   * insurance.bronze.payments exists and loads all payment Parquet files.
--   * Running this a second time loads 0 new rows (already-loaded files are skipped).
--   * (Stretch) add ingestion metadata via a follow-up MERGE or a SELECT in COPY INTO.
--
-- Exam domain 2 (Data Ingestion and Loading) — COPY INTO from cloud object storage.
-- Reference: docs/studybook/M2_bronze_ingestion.md

CREATE SCHEMA IF NOT EXISTS insurance.bronze;

-- TODO 1: create the (empty) target table the first time. One option is to let COPY INTO
--         infer the schema on first load; another is to declare it explicitly.
CREATE TABLE IF NOT EXISTS insurance.bronze.payments;

-- TODO 2: COPY INTO from the payments folder. Fill in the FILEFORMAT and options.
-- COPY INTO insurance.bronze.payments
-- FROM '/Volumes/insurance/landing/raw/payments'
-- FILEFORMAT = PARQUET
-- COPY_OPTIONS ('mergeSchema' = 'true');

-- TODO 3: verify idempotency — run the COPY INTO again and confirm 0 rows are added.
-- SELECT count(*) FROM insurance.bronze.payments;
