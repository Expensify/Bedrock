CREATE TABLE table1 ( indexedColumn, nonIndexedColumn );

WITH RECURSIVE
    randdata(x, y) AS (
        SELECT RANDOM(), RANDOM()
            UNION ALL
        SELECT RANDOM(), RANDOM() FROM randdata
        LIMIT 100*1500 --creates a 1.3MB db, adjust numbers to create larger or smaller
    )

INSERT INTO table1 ( indexedColumn, nonIndexedColumn )
    SELECT * FROM randdata;

CREATE INDEX perfTestIndexedColumn ON table1 ( indexedColumn );
