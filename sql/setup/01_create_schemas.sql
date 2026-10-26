
-- ============================================
-- PROJECT 11 - SEISMIC INTELLIGENCE LAKEHOUSE
-- UNITY CATALOG SCHEMA INITIALIZATION
-- ============================================

-- DEV
CREATE SCHEMA IF NOT EXISTS seismic_dev.bronze
COMMENT 'Raw USGS earthquake API data';

CREATE SCHEMA IF NOT EXISTS seismic_dev.silver
COMMENT 'Cleaned and standardized earthquake data';

CREATE SCHEMA IF NOT EXISTS seismic_dev.gold
COMMENT 'Analytics-ready seismic data';

CREATE SCHEMA IF NOT EXISTS seismic_dev.metadata
COMMENT 'Pipeline metadata, watermarks and execution logs';

CREATE SCHEMA IF NOT EXISTS seismic_dev.quarantine
COMMENT 'Invalid and rejected records';

-- TEST
CREATE SCHEMA IF NOT EXISTS seismic_test.bronze
COMMENT 'Raw USGS earthquake API data';

CREATE SCHEMA IF NOT EXISTS seismic_test.silver
COMMENT 'Cleaned and standardized earthquake data';

CREATE SCHEMA IF NOT EXISTS seismic_test.gold
COMMENT 'Analytics-ready seismic data';

CREATE SCHEMA IF NOT EXISTS seismic_test.metadata
COMMENT 'Pipeline metadata, watermarks and execution logs';

CREATE SCHEMA IF NOT EXISTS seismic_test.quarantine
COMMENT 'Invalid and rejected records';

-- PROD
CREATE SCHEMA IF NOT EXISTS seismic_prod.bronze
COMMENT 'Raw USGS earthquake API data';

CREATE SCHEMA IF NOT EXISTS seismic_prod.silver
COMMENT 'Cleaned and standardized earthquake data';

CREATE SCHEMA IF NOT EXISTS seismic_prod.gold
COMMENT 'Analytics-ready seismic data';

CREATE SCHEMA IF NOT EXISTS seismic_prod.metadata
COMMENT 'Pipeline metadata, watermarks and execution logs';

CREATE SCHEMA IF NOT EXISTS seismic_prod.quarantine
COMMENT 'Invalid and rejected records';
