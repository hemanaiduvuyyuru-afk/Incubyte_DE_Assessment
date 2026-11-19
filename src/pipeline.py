"""
Incubyte Data Engineer Technical Assessment.

SkyPoints Airline Loyalty ETL Pipeline.

Run:
    python -m src.pipeline --as-of 2026-11-19
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3

from datetime import date, datetime
from pathlib import Path

import pandas as pd


# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]

DATA = ROOT / "data"

DB = ROOT / "assessment.db"

SQL = ROOT / "sql"


# --------------------------------------------------
# 2. DATE PARSING
# --------------------------------------------------

def parse_date(value, *, style: str = "iso") -> str | None:

    if value is None or pd.isna(value):
        return None

    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime):
            return value.date().isoformat()

        return value.isoformat()

    s = str(value).strip()

    if not s or s.lower() in {
        "null", "none", "nan", "nat"
    }:
        return None

    formats = {
        "iso": [
            "%Y-%m-%d",
            "%Y%m%d"
        ],

        "us": [
            "%m/%d/%Y",
            "%m%d%Y"
        ],

        "ind": [
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y-%m-%d",
            "%Y%m%d"
        ],

        "aus": [
            "%Y-%m-%d",
            "%Y%m%d"
        ],

        "flat": [
            "%Y%m%d"
        ],

        "flat_dob": [
            "%d%m%Y"
        ]
    }

    # Handle dates such as 6152022 -> 06152022

    if (
        style in {"us", "flat_dob"}
        and s.isdigit()
        and len(s) == 7
    ):
        s = "0" + s

    for fmt in formats.get(style, formats["iso"]):

        try:
            return datetime.strptime(
                s, fmt
            ).date().isoformat()

        except ValueError:
            pass

    return None


# --------------------------------------------------
# 3. AGE CALCULATION
# --------------------------------------------------

def age_on(
    dob_iso: str | None,
    today: date
) -> int | None:

    if not dob_iso:
        return None

    birthday = date.fromisoformat(dob_iso)

    age = (
        today.year
        - birthday.year
        - (
            (today.month, today.day)
            < (birthday.month, birthday.day)
        )
    )

    return age


# --------------------------------------------------
# 4. CLEAN EMPTY VALUES
# --------------------------------------------------

def clean(value) -> str | None:

    if value is None or pd.isna(value):
        return None

    s = str(value).strip()

    if not s or s.lower() in {
        "null", "none", "nan"
    }:
        return None

    return s


# --------------------------------------------------
# 5. DATABASE INITIALIZATION
# --------------------------------------------------

def init_db(db: sqlite3.Connection) -> None:

    sql_file = SQL / "01_create_tables.sql"

    schema = sql_file.read_text(
        encoding="utf-8"
    )

    db.executescript(schema)


# --------------------------------------------------
# 6. FEED DATE
# --------------------------------------------------

def feed_date_for(path: Path) -> str:

    found = re.search(
        r"(?:19|20)\d{6}",
        path.stem
    )

    if found:

        result = parse_date(
            found.group(),
            style="flat"
        )

        if result:
            return result

    # Demo-only fallback.
    # Production should use source feed metadata.

    return "2024-01-15"


# --------------------------------------------------
# 7. SOURCE COLUMN NORMALIZATION
# --------------------------------------------------

def normalize(
    row: dict,
    country: str,
    namespace: str,
    style: str
) -> dict:

    def take(*names):

        for name in names:

            if name in row:
                return clean(row[name])

        return None

    return {
        "source_namespace": namespace,

        "member_id_raw": take(
            "ID",
            "Unique ID",
            "Member_Id",
            "Member ID"
        ),

        "member_name_raw": take(
            "Name",
            "Member Name",
            "Member N",
            "Member_Name"
        ),

        "dob_raw": take(
            "DOB",
            "Date of Birth"
        ),

        "tier_raw": take(
            "TierCode",
            "Tier Type",
            "Tier_Code"
        ),

        "enrollment_raw": take(
            "EnrollmentDate",
            "Enrollment",
            "Enrollmen",
            "Date of Enrollment",
            "Enrollment_Date"
        ),

        "last_flight_raw": take(
            "Flight Date",
            "FlightDate",
            "Date of Flight",
            "Last_Flight_Date"
        ),

        "agent_raw": take(
            "Agent_Name"
        ),

        "state_raw": take(
            "State"
        ),

        "country_raw": country,

        "postal_raw": take(
            "Post_Code"
        ),

        "active_raw": take(
            "Is_Active"
        ),

        "record_type": style
    }


# --------------------------------------------------
# 8. READ COUNTRY CSV FILES
# --------------------------------------------------

def read_country_file(
    path: Path,
    country: str
) -> list[dict]:

    if path.suffix.lower() == ".xlsx":

        frame = pd.read_excel(
            path,
            dtype=object,
            engine="openpyxl"
        )

    else:

        frame = pd.read_csv(
            path,
            dtype=object
        )

    frame.columns = [
        str(c).strip()
        for c in frame.columns
    ]

    style = {
        "IND": "ind",
        "USA": "us",
        "AUS": "aus"
    }[country]

    records = []

    for row in frame.to_dict("records"):

        normalized = normalize(
            row,
            country,
            country,
            style
        )

        records.append(normalized)

    return records


# --------------------------------------------------
# 9. READ FLAT MEMBER FILE
# --------------------------------------------------

def read_flat_file(path: Path) -> list[dict]:

    results = []

    fields = [
        "Member_Name",
        "Member_Id",
        "Enrollment_Date",
        "Last_Flight_Date",
        "Tier_Code",
        "Agent_Name",
        "State",
        "Country",
        "DOB",
        "Is_Active"
    ]

    lines = path.read_text(
        encoding="utf-8"
    ).splitlines()

    for line in lines:

        pieces = line.strip().split("|")

        if pieces and pieces[0] == "":
            pieces.pop(0)

        if not pieces or pieces[0] == "H":
            continue

        if pieces[0] != "D" or len(pieces) != 11:

            raise ValueError(
                f"Malformed flat-file record: {line}"
            )

        row = dict(
            zip(fields, pieces[1:])
        )

        normalized = normalize(
            row,
            row["Country"],
            "GLOBAL",
            "flat"
        )

        results.append(normalized)

    return results


# --------------------------------------------------
# 10. INGEST MEMBER DATA INTO RAW TABLE
# --------------------------------------------------

def ingest_members(db: sqlite3.Connection) -> None:

    sources = [
        (
            DATA / "IND(in).csv",
            "IND"
        ),
        (
            DATA / "USA(in).csv",
            "USA"
        ),
        (
            DATA / "AUS(Sheet1).csv",
            "AUS"
        ),
        (
            DATA / "members_flat.txt",
            "GLOBAL"
        )
    ]

    cols = [
        "source_file",
        "feed_date",
        "row_number",
        "source_namespace",
        "member_id_raw",
        "member_name_raw",
        "dob_raw",
        "tier_raw",
        "enrollment_raw",
        "last_flight_raw",
        "agent_raw",
        "state_raw",
        "country_raw",
        "postal_raw",
        "active_raw",
        "record_type"
    ]

    marks = ",".join(
        "?" for _ in cols
    )

    for path, country in sources:

        if not path.exists():

            print(
                f"Skipping missing file: {path.name}"
            )

            continue

        if country == "GLOBAL":

            records = read_flat_file(path)

        else:

            records = read_country_file(
                path,
                country
            )

        for i, rec in enumerate(
            records,
            start=1
        ):

            payload = {
                "source_file": path.name,
                "feed_date": feed_date_for(path),
                "row_number": i,
                **rec
            }

            query = f"""
                INSERT OR IGNORE INTO raw_members
                ({','.join(cols)})
                VALUES ({marks})
            """

            db.execute(
                query,
                [payload.get(c) for c in cols]
            )

        print(
            f"Read {len(records)} records from {path.name}"
        )

    db.commit()


# --------------------------------------------------
# 11. STAGING TRANSFORMATION AND VALIDATION
# --------------------------------------------------

def stage_members(
    db: sqlite3.Connection,
    today: date
) -> None:

    rows = db.execute(
        "SELECT * FROM raw_members ORDER BY raw_id"
    ).fetchall()

    for r in rows:

        style = r["record_type"]

        dob_style = (
            "flat_dob"
            if style == "flat"
            else style
        )

        dob = parse_date(
            r["dob_raw"],
            style=dob_style
        )

        enroll = parse_date(
            r["enrollment_raw"],
            style=style
        )

        flight = parse_date(
            r["last_flight_raw"],
            style=style
        )

        issues = []

        # Mandatory checks

        if not clean(r["member_id_raw"]):
            issues.append("missing member ID")

        if not clean(r["member_name_raw"]):
            issues.append("missing member name")

        if not enroll:
            issues.append(
                "missing/invalid enrollment date"
            )

        if not clean(r["country_raw"]):
            issues.append("missing country")

        # Country standardization

        country_value = (
            r["country_raw"] or ""
        ).upper()

        canonical_country = {
            "AU": "AUS"
        }.get(
            country_value,
            country_value
        )

        # Optional date validation

        date_fields = [
            ("DOB", dob, r["dob_raw"]),
            (
                "flight date",
                flight,
                r["last_flight_raw"]
            )
        ]

        for field, parsed, original in date_fields:

            if clean(original) and not parsed:

                issues.append(
                    f"invalid {field}"
                )

        # Future date validation

        if enroll:

            if date.fromisoformat(enroll) > today:

                issues.append(
                    "enrollment date in future"
                )

        if dob:

            if date.fromisoformat(dob) > today:

                issues.append(
                    "DOB in future"
                )

        # DOB must not be after enrollment

        if dob and enroll and dob > enroll:

            issues.append(
                "DOB later than enrollment"
            )

        if flight:

            if date.fromisoformat(flight) > today:

                issues.append(
                    "flight date in future"
                )

        # Reject invalid records

        if issues:

            db.execute(
                """
                DELETE FROM stg_members
                WHERE raw_id = ?
                """,
                (r["raw_id"],)
            )

            db.execute(
                """
                INSERT OR REPLACE INTO rejected_members
                VALUES (?, ?, ?)
                """,
                (
                    r["raw_id"],
                    r["source_file"],
                    "; ".join(issues)
                )
            )

            continue

        # Remove old rejection for valid rows

        db.execute(
            """
            DELETE FROM rejected_members
            WHERE raw_id = ?
            """,
            (r["raw_id"],)
        )

        # Stale member calculation

        stale = int(
            flight is not None
            and (
                today
                - date.fromisoformat(flight)
            ).days > 90
        )

        # Insert into staging table

        db.execute(
            """
            INSERT OR REPLACE INTO stg_members (
                raw_id,
                source_namespace,
                member_id,
                member_name,
                dob,
                age,
                tier_code,
                enrollment_date,
                last_flight_date,
                stale_member,
                agent_name,
                state,
                country,
                postal_code,
                is_active,
                feed_date
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                r["raw_id"],
                r["source_namespace"],
                r["member_id_raw"],
                r["member_name_raw"],
                dob,
                age_on(dob, today),
                r["tier_raw"],
                enroll,
                flight,
                stale,
                r["agent_raw"],
                r["state_raw"],
                canonical_country,
                r["postal_raw"],
                r["active_raw"],
                r["feed_date"]
            )
        )

    db.commit()


# --------------------------------------------------
# 12. LATEST MEMBER AND COUNTRY TABLES
# --------------------------------------------------

def rebuild_current_and_country(
    db: sqlite3.Connection
) -> None:

    transformation_sql = (
        SQL / "02_transformations.sql"
    ).read_text(encoding="utf-8")

    db.executescript(transformation_sql)

    # Remove previous country snapshots

    existing = db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name LIKE 'table_%'
        """
    ).fetchall()

    for (name,) in existing:

        if re.fullmatch(
            r"table_[A-Za-z0-9_]+",
            name
        ):

            db.execute(
                f'DROP TABLE IF EXISTS "{name}"'
            )

    # Identify countries

    countries = [
        r[0]
        for r in db.execute(
            """
            SELECT DISTINCT country
            FROM member_current
            """
        )
    ]

    # Create country-specific tables

    for country in countries:

        suffix = re.sub(
            r"[^A-Za-z0-9_]",
            "_",
            country.upper()
        )

        table = f"table_{suffix}"

        db.execute(
            f"""
            CREATE TABLE "{table}" AS
            SELECT *
            FROM member_current
            WHERE country = ?
            """,
            (country,)
        )

        db.execute(
            f"""
            CREATE UNIQUE INDEX "ux_{table}_member"
            ON "{table}" (
                source_namespace,
                member_id
            )
            """
        )

    db.commit()


# --------------------------------------------------
# 13. JSON REDEMPTION PROCESSING
# --------------------------------------------------

def ingest_redemptions(
    db: sqlite3.Connection
) -> None:

    path = DATA / "redemptions.json"

    if not path.exists():

        print(
            "Skipping missing redemptions.json"
        )

        return

    payload = path.read_text(
        encoding="utf-8"
    )

    # Store raw JSON

    db.execute(
        """
        INSERT OR IGNORE INTO raw_redemptions
        (source_file, payload)
        VALUES (?, ?)
        """,
        (
            path.name,
            payload
        )
    )

    try:

        feed = json.loads(payload)

        if not isinstance(feed, list):
            feed = [feed]

    except json.JSONDecodeError as err:

        db.execute(
            """
            INSERT INTO rejected_redemptions
            (source_file, reason, payload)
            VALUES (?, ?, ?)
            """,
            (
                path.name,
                str(err),
                payload
            )
        )

        db.commit()
        return

    # Flatten redemption transactions

    for entry in feed:

        member_id = clean(
            entry.get("member_id")
        )

        feed_dt = parse_date(
            entry.get("feed_date"),
            style="flat"
        )

        for tx in entry.get(
            "redemptions", []
        ):

            txid = clean(
                tx.get("txn_id")
            )

            txn_date = parse_date(
                tx.get("txn_date"),
                style="flat"
            )

            status = clean(
                tx.get("status")
            )

            miles = tx.get(
                "miles_redeemed"
            )

            try:

                miles_num = int(miles)

                if miles_num < 0:
                    raise ValueError(
                        "negative miles"
                    )

                if not (
                    member_id
                    and feed_dt
                    and txid
                    and txn_date
                    and status
                ):

                    raise ValueError(
                        "missing/invalid mandatory "
                        "transaction field"
                    )

                db.execute(
                    """
                    INSERT INTO redemption_transactions (
                        txn_id,
                        member_id,
                        feed_date,
                        txn_date,
                        partner,
                        miles_redeemed,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(txn_id)
                    DO UPDATE SET
                        member_id = excluded.member_id,
                        feed_date = excluded.feed_date,
                        txn_date = excluded.txn_date,
                        partner = excluded.partner,
                        miles_redeemed =
                            excluded.miles_redeemed,
                        status = excluded.status
                    """,
                    (
                        txid,
                        member_id,
                        feed_dt,
                        txn_date,
                        clean(
                            tx.get("partner")
                        ),
                        miles_num,
                        status
                    )
                )

            except (
                ValueError,
                TypeError
            ) as err:

                db.execute(
                    """
                    INSERT INTO rejected_redemptions
                    (source_file, reason, payload)
                    VALUES (?, ?, ?)
                    """,
                    (
                        path.name,
                        str(err),
                        json.dumps(tx)
                    )
                )

    db.commit()


# --------------------------------------------------
# 14. FINAL VALIDATION REPORT
# --------------------------------------------------

def report(db: sqlite3.Connection) -> None:

    print(
        "\n=== Validation summary ==="
    )

    tables = [
        "raw_members",
        "stg_members",
        "rejected_members",
        "member_current",
        "redemption_transactions",
        "rejected_redemptions"
    ]

    for table in tables:

        n = db.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        print(f"{table}: {n}")

    # Missing optional DOB

    missing_dob = db.execute(
        """
        SELECT COUNT(*)
        FROM stg_members
        WHERE dob IS NULL
        """
    ).fetchone()[0]

    print(
        "valid records missing optional DOB: "
        f"{missing_dob}"
    )

    # Country-wise count

    print("Country tables:")

    country_query = """
        SELECT country, COUNT(*)
        FROM member_current
        GROUP BY country
        ORDER BY country
    """

    for row in db.execute(country_query):

        print(
            f"  {row[0]}: {row[1]}"
        )

    # Rejected records

    print("Rejected member rows:")

    rejected_query = """
        SELECT source_file, errors
        FROM rejected_members
        ORDER BY raw_id
    """

    for row in db.execute(rejected_query):

        print(
            f"  {row[0]}: {row[1]}"
        )

    # Join members and transactions

    print(
        "\nSample join for global membership IDs:"
    )

    join_query = """
        SELECT
            m.member_name,
            m.member_id,
            r.txn_id,
            r.miles_redeemed
        FROM member_current m
        JOIN redemption_transactions r
            ON m.member_id = r.member_id
           AND m.source_namespace = 'GLOBAL'
        ORDER BY r.txn_id
        LIMIT 10
    """

    for row in db.execute(join_query):

        print(" ", tuple(row))


# --------------------------------------------------
# 15. MAIN FUNCTION
# --------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
        help="YYYY-MM-DD snapshot for age/staleness"
    )

    parser.add_argument(
        "--db",
        default=str(DB)
    )

    args = parser.parse_args()

    as_of = date.fromisoformat(
        args.as_of
    )

    with sqlite3.connect(
        args.db
    ) as db:

        db.row_factory = sqlite3.Row

        init_db(db)

        ingest_members(db)

        stage_members(
            db,
            as_of
        )

        rebuild_current_and_country(db)

        ingest_redemptions(db)

        report(db)


# --------------------------------------------------
# 16. PROGRAM ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    main()