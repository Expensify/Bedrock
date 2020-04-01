DROP TABLE IF EXISTS perfTest;

CREATE TABLE perfTest ( indexedColumn, nonIndexedColumn );

WITH RECURSIVE
    randdata(x, y) AS (
        SELECT RANDOM(), RANDOM()
            UNION ALL
        SELECT RANDOM(), RANDOM() FROM randdata
        LIMIT 100000*100000*1 -- makes 10b row, roughly a 250gb, database
    )

INSERT INTO perfTest ( indexedColumn, nonIndexedColumn )
    SELECT * FROM randdata;

CREATE INDEX perfTestIndexedColumn ON perfTest ( indexedColumn );
