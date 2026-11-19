
PRAGMA foreign_keys = ON;

-- 1. RAW MEMBER TABLE
CREATE TABLE IF NOT EXISTS raw_members (
    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    source_namespace TEXT NOT NULL,
    feed_date TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    member_id_raw TEXT,
    member_name_raw TEXT,
    dob_raw TEXT,
    tier_raw TEXT,
    enrollment_raw TEXT,
    last_flight_raw TEXT,
    agent_raw TEXT,
    state_raw TEXT,
    country_raw TEXT,
    postal_raw TEXT,
    active_raw TEXT,
    record_type TEXT,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_file, feed_date, row_number)
);

-- 2. STAGING MEMBER TABLE
CREATE TABLE IF NOT EXISTS stg_members (
    raw_id INTEGER PRIMARY KEY,
    source_namespace TEXT NOT NULL,
    member_id TEXT NOT NULL,
    member_name TEXT NOT NULL,
    dob TEXT,
    age INTEGER,
    tier_code TEXT,
    enrollment_date TEXT NOT NULL,
    last_flight_date TEXT,
    stale_member INTEGER NOT NULL,
    agent_name TEXT,
    state TEXT,
    country TEXT NOT NULL,
    postal_code TEXT,
    is_active TEXT,
    feed_date TEXT NOT NULL
);

-- 3. REJECTED MEMBERS
CREATE TABLE IF NOT EXISTS rejected_members (
    raw_id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL,
    errors TEXT NOT NULL
);

-- 4. RAW REDEMPTIONS
CREATE TABLE IF NOT EXISTS raw_redemptions (
    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    payload TEXT NOT NULL,
    loaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_file, payload)
);

-- 5. REDEMPTION TRANSACTIONS
CREATE TABLE IF NOT EXISTS redemption_transactions (
    txn_id TEXT PRIMARY KEY,
    member_id TEXT NOT NULL,
    feed_date TEXT,
    txn_date TEXT,
    partner TEXT,
    miles_redeemed INTEGER NOT NULL,
    status TEXT NOT NULL
);

-- 6. REJECTED REDEMPTIONS
CREATE TABLE IF NOT EXISTS rejected_redemptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL
);

-- 7. CURRENT MEMBER TABLE
CREATE TABLE IF NOT EXISTS member_current (
    source_namespace TEXT NOT NULL,
    member_id TEXT NOT NULL,
    raw_id INTEGER NOT NULL,
    member_name TEXT NOT NULL,
    country TEXT NOT NULL,
    dob TEXT,
    age INTEGER,
    tier_code TEXT,
    enrollment_date TEXT,
    last_flight_date TEXT,
    stale_member INTEGER NOT NULL,
    feed_date TEXT NOT NULL,
    PRIMARY KEY (source_namespace, member_id)
);

-- 8. INDEXES
CREATE INDEX IF NOT EXISTS idx_stage_identity
ON stg_members(source_namespace, member_id, feed_date);

CREATE INDEX IF NOT EXISTS idx_member_country
ON member_current(country);

CREATE INDEX IF NOT EXISTS idx_redemption_member
ON redemption_transactions(member_id);
