"""
contacts_extract.py - Progressive Gmail inbox contact extraction

Fetches ALL inbox emails in batches of 200, writing contacts to crm.db
after each batch so the UI can show progress in real time.

Progress is tracked in the settings table so the frontend can poll it.

Usage:
  python3 gmail_auth.py   # first time only
  python3 contacts_extract.py
"""

import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

load_dotenv()

TOKEN_FILE = "token.json"
DB_FILE    = os.environ.get("DATABASE_PATH", "crm.db")

BATCH_SIZE    = 200
FETCH_WORKERS = 10

# Domains that are always mailing lists / automated, never individual people
NEWSLETTER_DOMAINS = {
    "substack.com", "beehiiv.com", "mailchimp.com", "constantcontact.com",
    "sendgrid.net", "mailgun.org", "klaviyo.com", "hubspot.com",
    "convertkit.com", "campaignmonitor.com", "getresponse.com",
    "quora.com", "neighborhoodalerts.com", "beehi.ve",
    "linkedin.com", "facebookmail.com", "twitter.com", "x.com",
    "accounts.google.com", "noreply.github.com",
}

AUTOMATED_ADDR_RE = re.compile(
    r"(no.?reply|do.?not.?reply|mailer.?daemon|postmaster|"
    r"\bdigest\b|\balerts?\b|\bnotifications?\b|\bnewsletter\b|\binfo@\b|"
    r"\bsupport@\b|\bteam@\b|\bmarketing@\b|\bnews@\b|\bupdates?@\b)",
    re.IGNORECASE,
)


# ── Database ──────────────────────────────────────────────────────────────────

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT,
            email       TEXT UNIQUE,
            first_seen  TEXT,
            last_seen   TEXT,
            email_count INTEGER DEFAULT 1,
            topics      TEXT
        )
    """)
    for col, default in [
        ("company", "''"), ("how_met", "''"),
        ("relationship_tags", "'[]'"), ("notes", "''"),
        ("reminder_date", "NULL"),
        ("last_gmail_id", "NULL"),
        ("last_received_date", "NULL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE contacts ADD COLUMN {col} TEXT DEFAULT {default}")
        except Exception:
            pass
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()


def set_progress(conn, fetched, total_est, contacts_found, status="syncing"):
    """Write sync progress to settings so the UI can poll it."""
    progress = json.dumps({
        "status": status,
        "fetched": fetched,
        "total_est": total_est,
        "contacts": contacts_found,
    })
    conn.execute(
        "INSERT INTO settings (key, value) VALUES ('sync_progress', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (progress,)
    )
    conn.commit()


def upsert_contact(conn, name, email, date_str, subject, gmail_id=None):
    dt = parse_date(date_str)
    iso_date = dt.isoformat() if dt else date_str

    existing = conn.execute(
        "SELECT id, email_count, topics, first_seen, last_seen, last_gmail_id, last_received_date FROM contacts WHERE email = ?",
        (email,)
    ).fetchone()

    if existing:
        contact_id, count, topics_json, first_seen, last_seen, old_gmail_id, old_received = existing
        topics = json.loads(topics_json or "[]")
        norm_subj = re.sub(r'^(re|fw|fwd)\s*:\s*', '', subject or "", flags=re.IGNORECASE).strip().lower()
        if subject and norm_subj:
            # Remove older version of this thread so the most recent lands at the end
            topics = [t for t in topics
                      if re.sub(r'^(re|fw|fwd)\s*:\s*', '', t, flags=re.IGNORECASE).strip().lower() != norm_subj]
            topics.append(subject)

        new_first = min(first_seen, iso_date) if first_seen and iso_date else first_seen or iso_date
        new_last = max(last_seen, iso_date) if last_seen and iso_date else last_seen or iso_date
        new_gmail_id = gmail_id if (iso_date == new_last and gmail_id) else old_gmail_id
        new_received = max(old_received, iso_date) if old_received and iso_date else iso_date or old_received

        conn.execute(
            "UPDATE contacts SET name=?, first_seen=?, last_seen=?, email_count=?, topics=?, last_gmail_id=?, last_received_date=? WHERE id=?",
            (name, new_first, new_last, count + 1, json.dumps(topics[-20:]), new_gmail_id, new_received, contact_id),
        )
    else:
        conn.execute(
            "INSERT INTO contacts (name, email, first_seen, last_seen, email_count, topics, last_gmail_id, last_received_date) "
            "VALUES (?,?,?,?,1,?,?,?)",
            (name, email, iso_date, iso_date, json.dumps([subject] if subject else []), gmail_id, iso_date),
        )


# ── Gmail ─────────────────────────────────────────────────────────────────────

def get_gmail_service():
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"Missing {TOKEN_FILE}. Run gmail_auth.py first.")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE)
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def get_my_email(service):
    return service.users().getProfile(userId="me").execute()["emailAddress"].lower()


def parse_sender(raw):
    match = re.match(r'^"?([^"<]+)"?\s*<([^>]+)>', raw)
    if match:
        return match.group(1).strip(), match.group(2).strip().lower()
    email = raw.strip().lower()
    return email.split("@")[0].replace(".", " ").title(), email


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def is_real_person_address(email_addr):
    if AUTOMATED_ADDR_RE.search(email_addr):
        return False
    domain = email_addr.split("@")[-1] if "@" in email_addr else ""
    return not any(domain == d or domain.endswith("." + d) for d in NEWSLETTER_DOMAINS)


def is_personal_email(label_ids):
    dominated = {"CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_SOCIAL", "CATEGORY_FORUMS"}
    return not any(lbl in label_ids for lbl in dominated)


def fetch_one(creds, msg_id):
    svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
    full = svc.users().messages().get(
        userId="me", id=msg_id,
        format="metadata",
        metadataHeaders=["From", "To", "Cc", "Subject", "Date"],
    ).execute()
    headers = {h["name"]: h["value"] for h in full["payload"]["headers"]}
    label_ids = full.get("labelIds", [])
    return {
        "id":        full["id"],
        "from":      headers.get("From", ""),
        "to":        headers.get("To", ""),
        "cc":        headers.get("Cc", ""),
        "subject":   headers.get("Subject", ""),
        "date":      headers.get("Date", ""),
        "label_ids": label_ids,
    }


def fetch_batch_metadata(msg_ids):
    """Fetch metadata for a batch of message IDs in parallel."""
    creds = Credentials.from_authorized_user_file(TOKEN_FILE)
    emails = [None] * len(msg_ids)
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        futures = {pool.submit(fetch_one, creds, mid): i for i, mid in enumerate(msg_ids)}
        for future in as_completed(futures):
            try:
                emails[futures[future]] = future.result()
            except Exception as e:
                print(f"  Error fetching message: {e}")
    return [e for e in emails if e is not None]


def fetch_all_message_ids(service, query):
    """Fetch ALL message IDs matching query (paginate through everything)."""
    msg_ids = []
    page_token = None
    while True:
        results = service.users().messages().list(
            userId="me", maxResults=500, q=query,
            pageToken=page_token,
        ).execute()
        messages = results.get("messages", [])
        if not messages:
            break
        msg_ids.extend(m["id"] for m in messages)
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return msg_ids


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Gmail...")
    service  = get_gmail_service()
    my_email = get_my_email(service)
    print(f"Authenticated as: {my_email}\n")

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    # Check if this is an incremental sync (last_synced_at exists)
    row = conn.execute("SELECT value FROM settings WHERE key = 'last_synced_at'").fetchone()
    is_incremental = row is not None and row[0] is not None

    if not is_incremental:
        # First sync: reset topics and fetch everything
        print("First sync — fetching all emails...")
        conn.execute("UPDATE contacts SET topics = '[]' WHERE contact_source = 'gmail' OR contact_source IS NULL")
        conn.commit()
        inbox_query = "category:primary"
    else:
        # Incremental sync: only fetch emails since last sync
        last_synced = row[0][:10].replace("-", "/")  # "2026-03-15" → "2026/03/15"
        inbox_query = f"category:primary after:{last_synced}"
        print(f"Incremental sync — fetching emails after {last_synced}...")

    # Step 1: Get message IDs
    all_ids = fetch_all_message_ids(service, inbox_query)
    total = len(all_ids)
    print(f"  Found {total} inbox messages\n")

    set_progress(conn, 0, total, 0, "syncing")

    # Step 2: Process in batches of BATCH_SIZE
    total_contacts = 0
    total_fetched = 0
    skipped_auto = 0
    skipped_cat = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch_ids = all_ids[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Batch {batch_num}/{total_batches}: fetching {len(batch_ids)} messages...")

        emails = fetch_batch_metadata(batch_ids)
        total_fetched += len(emails)

        # Process oldest first within batch
        for email in reversed(emails):
            subject  = email["subject"]
            date_str = email["date"]

            if not is_personal_email(email["label_ids"]):
                skipped_cat += 1
                continue

            name, addr = parse_sender(email["from"])
            if addr == my_email:
                continue
            if not is_real_person_address(addr):
                skipped_auto += 1
                continue

            upsert_contact(conn, name, addr, date_str, subject, email["id"])
            total_contacts += 1

        # Commit after each batch so contacts appear in the UI immediately
        conn.commit()

        # Update progress
        contacts_in_db = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE contact_source = 'gmail' OR contact_source IS NULL"
        ).fetchone()[0]
        set_progress(conn, total_fetched, total, contacts_in_db, "syncing")
        print(f"  Batch done — {total_fetched}/{total} fetched, {contacts_in_db} contacts so far")

    # Final progress
    final_contacts = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE contact_source = 'gmail' OR contact_source IS NULL"
    ).fetchone()[0]
    set_progress(conn, total_fetched, total, final_contacts, "done")

    conn.close()

    print(f"\nSync complete:")
    print(f"  {total_fetched} emails processed")
    print(f"  {total_contacts} contact entries recorded")
    print(f"  {skipped_cat} skipped (non-personal category)")
    print(f"  {skipped_auto} skipped (automated/newsletter)")
    print(f"  {final_contacts} unique Gmail contacts in database")


if __name__ == "__main__":
    main()
