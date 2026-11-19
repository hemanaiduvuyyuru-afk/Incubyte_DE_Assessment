-- 1. Rejected records

SELECT
    source_file,
    errors
FROM rejected_members
ORDER BY raw_id;


-- 2. Duplicate records in staging

SELECT
    source_namespace,
    member_id,
    COUNT(*) AS records
FROM stg_members
GROUP BY
    source_namespace,
    member_id
HAVING COUNT(*) > 1;


-- 3. Duplicate current member keys

SELECT
    source_namespace,
    member_id,
    COUNT(*) AS records
FROM member_current
GROUP BY
    source_namespace,
    member_id
HAVING COUNT(*) > 1;


-- 4. Country-wise member count

SELECT
    country,
    COUNT(*) AS members
FROM member_current
GROUP BY country
ORDER BY country;


-- 5. Raw to staging reconciliation

SELECT
    (SELECT COUNT(*) FROM raw_members)
        AS raw_count,

    (SELECT COUNT(*) FROM stg_members)
        AS staged_count,

    (SELECT COUNT(*) FROM rejected_members)
        AS rejected_count;