DROP TABLE IF EXISTS perfTest;

CREATE TABLE perfTest ( indexedColumn, nonIndexedColumn );

WITH RECURSIVE
    randdata(x, y) AS (
        SELECT RANDOM(), RANDOM()
            UNION ALL
        SELECT RANDOM(), RANDOM() FROM randdata
        LIMIT 1000*1000*1000*30
    )

INSERT INTO perfTest ( indexedColumn, nonIndexedColumn )
    SELECT * FROM randdata;

CREATE INDEX perfTestIndexedColumn ON perfTest ( indexedColumn );

