DROP TABLE IF EXISTS perfTest;

CREATE TABLE perfTest ( indexedColumn, nonIndexedColumn );

/* Generate 20000000 rows of random data that is 50% likely to be under 20000000*2 */
WITH RECURSIVE
    randdata(x, y) AS (
        SELECT ABS(RANDOM())%(20000000*2), ABS(RANDOM())%(20000000*2)
            UNION ALL
        SELECT ABS(RANDOM())%(20000000*2), ABS(RANDOM())%(20000000*2) FROM randdata
        LIMIT 20000000
    )

INSERT INTO perfTest ( indexedColumn, nonIndexedColumn )
    SELECT * FROM randdata;

CREATE INDEX perfTestIndexedColumn ON perfTest ( indexedColumn );

