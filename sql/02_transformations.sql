DELETE FROM member_current;

INSERT INTO member_current (
    source_namespace,
    member_id,
    raw_id,
    member_name,
    country,
    dob,
    age,
    tier_code,
    enrollment_date,
    last_flight_date,
    stale_member,
    feed_date
)
SELECT
    source_namespace,
    member_id,
    raw_id,
    member_name,
    country,
    dob,
    age,
    tier_code,
    enrollment_date,
    last_flight_date,
    stale_member,
    feed_date
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY source_namespace, member_id
            ORDER BY feed_date DESC, raw_id DESC
        ) AS rn
    FROM stg_members s
)
WHERE rn = 1;