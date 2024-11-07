CREATE SCHEMA IF NOT EXISTS test_psql_copy;

USE test_psql_copy;

CREATE TABLE t (a int, b text);

\copy t FROM 'pgtest/testdata/basic.csv' WITH DELIMITER ',' CSV HEADER;