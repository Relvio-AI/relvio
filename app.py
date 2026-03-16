"""
app.py - Orbit CRM web dashboard
Run: make run  →  http://localhost:5000
"""

import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import secrets as _secrets

from dotenv import load_dotenv, set_key
from flask import Flask, render_template, jsonify, request, redirect, url_for

load_dotenv()

# ── Auto-generate SECRET_KEY if missing ───────────────────────────────────────
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if not os.environ.get("SECRET_KEY"):
    if not os.path.exists(_env_path):
        # Create .env from .env.example if it doesn't exist
        example = os.path.join(os.path.dirname(_env_path), ".env.example")
        if os.path.exists(example):
            import shutil
            shutil.copy(example, _env_path)
        else:
            with open(_env_path, "w") as f:
                f.write("")
    generated_key = _secrets.token_hex(32)
    set_key(_env_path, "SECRET_KEY", generated_key)
    os.environ["SECRET_KEY"] = generated_key
    load_dotenv(override=True)

app = Flask(__name__)
app.secret_key = os.environ["SECRET_KEY"]
DB_FILE = os.environ.get("DATABASE_PATH", "crm.db")

PALETTE_NAMES = ["purple", "teal", "amber", "red", "pink", "blue", "green"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    # ── Base tables (created on first run) ────────────────────────────────────
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id  INTEGER NOT NULL,
            type        TEXT NOT NULL,
            note        TEXT DEFAULT '',
            date        TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS important_dates (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id  INTEGER NOT NULL,
            label       TEXT NOT NULL,
            date        TEXT NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contact_relationships (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id_a    INTEGER NOT NULL,
            contact_id_b    INTEGER NOT NULL,
            relationship    TEXT NOT NULL,
            label           TEXT DEFAULT '',
            FOREIGN KEY (contact_id_a) REFERENCES contacts(id) ON DELETE CASCADE,
            FOREIGN KEY (contact_id_b) REFERENCES contacts(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE NOT NULL,
            notes       TEXT DEFAULT '',
            website     TEXT DEFAULT ''
        )
    """)
    conn.commit()

    # ── Column migrations (safe to re-run) ────────────────────────────────────
    migrations = [
        "ALTER TABLE contacts ADD COLUMN company TEXT DEFAULT ''",
        "ALTER TABLE contacts ADD COLUMN how_met TEXT DEFAULT ''",
        "ALTER TABLE contacts ADD COLUMN relationship_tags TEXT DEFAULT '[]'",
        "ALTER TABLE contacts ADD COLUMN notes TEXT DEFAULT ''",
        "ALTER TABLE contacts ADD COLUMN reminder_date TEXT",
        "ALTER TABLE contacts ADD COLUMN health_score TEXT DEFAULT 'warm'",
        "ALTER TABLE contacts ADD COLUMN health_updated_at DATETIME",
        "ALTER TABLE contacts ADD COLUMN last_gmail_id TEXT",
        "ALTER TABLE contacts ADD COLUMN contact_source TEXT DEFAULT 'gmail'",
        "ALTER TABLE contacts ADD COLUMN last_received_date TEXT",
        "ALTER TABLE contacts ADD COLUMN birthday TEXT",
        "ALTER TABLE contacts ADD COLUMN archived INTEGER DEFAULT 0",
        "ALTER TABLE contacts ADD COLUMN company_id INTEGER",
        "ALTER TABLE contacts ADD COLUMN phone TEXT DEFAULT ''",
        "ALTER TABLE contacts ADD COLUMN linkedin TEXT DEFAULT ''",
        "ALTER TABLE contacts ADD COLUMN title TEXT DEFAULT ''",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass

    # One-time migration: backfill company_id from text company column
    try:
        orphans = conn.execute(
            "SELECT DISTINCT company FROM contacts WHERE company != '' AND company_id IS NULL"
        ).fetchall()
        for row in orphans:
            conn.execute("INSERT OR IGNORE INTO companies (name) VALUES (?)", (row["company"],))
        conn.execute("""
            UPDATE contacts SET company_id = (
                SELECT id FROM companies WHERE companies.name = contacts.company
            ) WHERE company != '' AND company_id IS NULL
        """)
        conn.commit()
    except Exception:
        pass

    # Backfill contact_source for existing LinkedIn contacts
    try:
        conn.execute("""
            UPDATE contacts SET contact_source = 'linkedin'
            WHERE contact_source = 'gmail'
              AND (email LIKE '%@linkedin.placeholder' OR how_met = 'LinkedIn')
              AND last_gmail_id IS NULL
        """)
        conn.execute("""
            UPDATE contacts SET contact_source = 'both'
            WHERE contact_source = 'gmail'
              AND (email LIKE '%@linkedin.placeholder' OR how_met = 'LinkedIn')
              AND last_gmail_id IS NOT NULL
        """)
        conn.commit()
    except Exception:
        pass

    return conn


def initials(name):
    parts = (name or "").strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return name[:2].upper() if name else "?"


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


def relative_sync(ts_str):
    if not ts_str:
        return "Never synced"
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = int((datetime.now(timezone.utc) - dt).total_seconds() / 60)
        if mins < 1:  return "Just now"
        if mins < 60: return f"{mins}m ago"
        return f"{mins // 60}h ago"
    except Exception:
        return ""


def normalize_subject(s):
    return re.sub(r'^(re|fw|fwd)\s*:\s*', '', s or "", flags=re.IGNORECASE).strip().lower()


def gmail_url(mid):
    return f"https://mail.google.com/mail/u/0/#inbox/{mid}"



# ── Setup helpers ─────────────────────────────────────────────────────────────

def _google_creds_configured():
    cid = os.environ.get("GOOGLE_CLIENT_ID", "")
    return cid and cid != "your-google-client-id"


def _needs_setup():
    return not _google_creds_configured() or not _gmail_connected()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    if _needs_setup():
        return redirect(url_for("setup"))
    return contacts_view()


@app.route("/setup", methods=["GET", "POST"])
def setup():
    error = request.args.get("error")
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        client_secret = request.form.get("client_secret", "").strip()
        if not client_id or not client_secret:
            return jsonify({"ok": False, "error": "Both Client ID and Client Secret are required."})
        set_key(_env_path, "GOOGLE_CLIENT_ID", client_id)
        set_key(_env_path, "GOOGLE_CLIENT_SECRET", client_secret)
        os.environ["GOOGLE_CLIENT_ID"] = client_id
        os.environ["GOOGLE_CLIENT_SECRET"] = client_secret
        return jsonify({"ok": True})
    return render_template("setup.html", error=error,
                           gmail_connected=_gmail_connected(),
                           google_configured=_google_creds_configured(),
                           google_client_id=os.environ.get("GOOGLE_CLIENT_ID", "") if _google_creds_configured() else "",
                           google_client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", "") if _google_creds_configured() else "")


@app.route("/oauth/start", methods=["POST"])
def oauth_start():
    """Launch gmail_auth.py in the background — it opens the browser for consent."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gmail_auth.py")
    subprocess.Popen(
        [sys.executable, script],
        env=os.environ.copy(),
    )
    return jsonify({"ok": True})


@app.route("/oauth/status")
def oauth_status():
    """Check if token.json exists yet."""
    return jsonify({"connected": _gmail_connected()})



@app.route("/sync", methods=["POST"])
def sync():
    global _sync_running
    if not _gmail_connected():
        return jsonify({"ok": False, "error": "Gmail not connected. Run python3 gmail_auth.py first."})
    with _sync_lock:
        if _sync_running:
            return jsonify({"ok": True, "status": "running"})
        _sync_running = True
    t = threading.Thread(target=_run_sync, daemon=True)
    t.start()
    return jsonify({"ok": True, "status": "started"})


@app.route("/sync-status")
def sync_status():
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key = 'last_synced_at'").fetchone()
    prog_row = conn.execute("SELECT value FROM settings WHERE key = 'sync_progress'").fetchone()
    conn.close()
    last = row["value"] if row else None
    progress = None
    if prog_row and prog_row["value"]:
        try:
            progress = json.loads(prog_row["value"])
        except Exception:
            pass
    return jsonify({
        "running": _sync_running,
        "last_synced_at": last,
        "last_synced_label": relative_sync(last),
        "progress": progress,
    })


# ── Contact routes ────────────────────────────────────────────────────────────

@app.route("/contacts/<int:contact_id>", methods=["GET"])
def get_contact(contact_id):
    conn = get_conn()
    r = conn.execute(
        "SELECT id, name, email, company, how_met, relationship_tags, "
        "notes, reminder_date, email_count, last_seen, topics, "
        "phone, linkedin, title, birthday "
        "FROM contacts WHERE id = ?", (contact_id,)
    ).fetchone()

    if not r:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    # Build thread list from topics (subjects extracted by contacts_extract)
    threads = []
    seen_subjects = set()
    topics = json.loads(r["topics"] or "[]")
    contact_email = r["email"] or ""
    for subj in reversed(topics[-10:]):
        norm = normalize_subject(subj)
        if norm in seen_subjects:
            continue
        seen_subjects.add(norm)
        threads.append({
            "subject": subj or "(no subject)",
            "date":    "",
            "url":     f"https://mail.google.com/mail/u/0/#search/{contact_email}+subject%3A{subj.replace(' ', '+')[:40]}",
        })
    threads = threads[:10]

    # Activities
    activities = []
    for a in conn.execute(
        "SELECT id, type, note, date FROM activities WHERE contact_id = ? ORDER BY date DESC LIMIT 20",
        (contact_id,)
    ).fetchall():
        activities.append({"id": a["id"], "type": a["type"], "note": a["note"], "date": a["date"]})

    # Relationships
    relationships = []
    for rel in conn.execute("""
        SELECT cr.id, cr.contact_id_a, cr.contact_id_b, cr.relationship, cr.label,
               ca.name as name_a, cb.name as name_b
        FROM contact_relationships cr
        JOIN contacts ca ON ca.id = cr.contact_id_a
        JOIN contacts cb ON cb.id = cr.contact_id_b
        WHERE cr.contact_id_a = ? OR cr.contact_id_b = ?
    """, (contact_id, contact_id)).fetchall():
        other_id = rel["contact_id_b"] if rel["contact_id_a"] == contact_id else rel["contact_id_a"]
        other_name = rel["name_b"] if rel["contact_id_a"] == contact_id else rel["name_a"]
        relationships.append({
            "id": rel["id"], "other_id": other_id, "other_name": other_name,
            "relationship": rel["relationship"], "label": rel["label"] or "",
        })

    # Important dates
    imp_dates = []
    for d in conn.execute(
        "SELECT id, label, date FROM important_dates WHERE contact_id = ?", (contact_id,)
    ).fetchall():
        imp_dates.append({"id": d["id"], "label": d["label"], "date": d["date"]})

    birthday = ""
    try:
        birthday = r["birthday"] or ""
    except (IndexError, KeyError):
        pass

    conn.close()
    return jsonify({
        "id":                r["id"],
        "name":              r["name"] or "",
        "email":             r["email"] or "",
        "company":           r["company"] or "",
        "how_met":           r["how_met"] or "",
        "relationship_tags": json.loads(r["relationship_tags"] or "[]"),
        "notes":             r["notes"] or "",
        "reminder_date":     r["reminder_date"] or "",
        "email_count":       r["email_count"],
        "threads":           threads,
        "activities":        activities,
        "relationships":     relationships,
        "important_dates":   imp_dates,
        "birthday":          birthday,
        "phone":             r["phone"] or "",
        "linkedin":          r["linkedin"] or "",
        "title":             r["title"] or "",
    })


@app.route("/contacts/<int:contact_id>/update", methods=["POST"])
def update_contact(contact_id):
    data = request.json or {}
    conn = get_conn()

    # Handle company linking
    company_name = data.get("company", "").strip()
    company_id = None
    if company_name:
        conn.execute("INSERT OR IGNORE INTO companies (name) VALUES (?)", (company_name,))
        row = conn.execute("SELECT id FROM companies WHERE name = ?", (company_name,)).fetchone()
        if row:
            company_id = row["id"]

    conn.execute("""
        UPDATE contacts SET
            name = ?, company = ?, company_id = ?, how_met = ?,
            relationship_tags = ?, notes = ?, reminder_date = ?, birthday = ?,
            phone = ?, linkedin = ?, title = ?
        WHERE id = ?
    """, (
        data.get("name", ""),
        company_name,
        company_id,
        data.get("how_met", ""),
        json.dumps(data.get("relationship_tags", [])),
        data.get("notes", ""),
        data.get("reminder_date") or None,
        data.get("birthday") or None,
        data.get("phone", ""),
        data.get("linkedin", ""),
        data.get("title", ""),
        contact_id,
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/delete", methods=["POST"])
def delete_contact(contact_id):
    conn = get_conn()
    conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.execute("DELETE FROM activities WHERE contact_id = ?", (contact_id,))
    conn.execute("DELETE FROM important_dates WHERE contact_id = ?", (contact_id,))
    conn.execute("DELETE FROM contact_relationships WHERE contact_id_a = ? OR contact_id_b = ?",
                 (contact_id, contact_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/archive", methods=["POST"])
def archive_contact(contact_id):
    conn = get_conn()
    conn.execute("UPDATE contacts SET archived = 1 WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/unarchive", methods=["POST"])
def unarchive_contact(contact_id):
    conn = get_conn()
    conn.execute("UPDATE contacts SET archived = 0 WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/activities", methods=["POST"])
def add_activity(contact_id):
    data = request.json or {}
    conn = get_conn()
    act_date = data.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn.execute(
        "INSERT INTO activities (contact_id, type, note, date, created_at) VALUES (?,?,?,?,?)",
        (contact_id, data.get("type", "other"), data.get("note", ""), act_date,
         datetime.now(timezone.utc).isoformat()),
    )
    # Update last_seen to reset health timer
    iso_now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE contacts SET last_seen = MAX(COALESCE(last_seen, ''), ?)
        WHERE id = ?
    """, (iso_now, contact_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/dates", methods=["POST"])
def add_important_date(contact_id):
    data = request.json or {}
    conn = get_conn()
    conn.execute(
        "INSERT INTO important_dates (contact_id, label, date) VALUES (?,?,?)",
        (contact_id, data.get("label", ""), data.get("date", "")),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/dates/<int:date_id>/delete", methods=["POST"])
def delete_important_date(contact_id, date_id):
    conn = get_conn()
    conn.execute("DELETE FROM important_dates WHERE id = ? AND contact_id = ?", (date_id, contact_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/<int:contact_id>/relationships", methods=["POST"])
def add_relationship(contact_id):
    data = request.json or {}
    conn = get_conn()
    conn.execute(
        "INSERT INTO contact_relationships (contact_id_a, contact_id_b, relationship, label) VALUES (?,?,?,?)",
        (contact_id, data.get("other_contact_id"), data.get("relationship", ""), data.get("label", "")),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/contacts/relationships/<int:rel_id>/delete", methods=["POST"])
def delete_relationship(rel_id):
    conn = get_conn()
    conn.execute("DELETE FROM contact_relationships WHERE id = ?", (rel_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/settings/warmth", methods=["GET"])
def get_warmth_settings():
    thresholds = get_warmth_thresholds()
    return jsonify(thresholds)


@app.route("/settings/warmth", methods=["POST"])
def save_warmth_settings():
    data = request.json or {}
    thresholds = {
        "healthy": max(1, int(data.get("healthy", 7))),
        "warm": max(1, int(data.get("warm", 30))),
        "cold": max(1, int(data.get("cold", 60))),
    }
    # Ensure healthy < warm < cold
    if thresholds["warm"] <= thresholds["healthy"]:
        thresholds["warm"] = thresholds["healthy"] + 1
    if thresholds["cold"] <= thresholds["warm"]:
        thresholds["cold"] = thresholds["warm"] + 1
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings (key, value) VALUES ('warmth_thresholds', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (json.dumps(thresholds),)
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "thresholds": thresholds})


@app.route("/contacts/search")
def search_contacts():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, name, email FROM contacts WHERE (name LIKE ? OR email LIKE ?) AND archived = 0 LIMIT 8",
        (f"%{q}%", f"%{q}%"),
    ).fetchall()
    conn.close()
    return jsonify([{"id": r["id"], "name": r["name"], "email": r["email"]} for r in rows])


@app.route("/companies")
def list_companies():
    conn = get_conn()
    rows = conn.execute("""
        SELECT c.id, c.name, c.website, c.notes, COUNT(ct.id) as contact_count
        FROM companies c LEFT JOIN contacts ct ON ct.company_id = c.id AND ct.archived = 0
        GROUP BY c.id ORDER BY c.name
    """).fetchall()
    conn.close()
    return jsonify([{
        "id": r["id"], "name": r["name"], "website": r["website"],
        "notes": r["notes"], "contact_count": r["contact_count"],
    } for r in rows])


@app.route("/companies/<int:company_id>")
def get_company(company_id):
    conn = get_conn()
    co = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
    if not co:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    contacts = conn.execute(
        "SELECT id, name, email FROM contacts WHERE company_id = ? AND archived = 0", (company_id,)
    ).fetchall()
    conn.close()
    return jsonify({
        "id": co["id"], "name": co["name"], "website": co["website"], "notes": co["notes"],
        "contacts": [{"id": c["id"], "name": c["name"], "email": c["email"]} for c in contacts],
    })


@app.route("/contacts/import", methods=["POST"])
def import_contacts():
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    added, skipped, errors = 0, 0, []

    # Manual JSON entry
    if request.is_json:
        data = request.json or {}
        name  = (data.get("name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        if not name or not email or "@" not in email:
            conn.close()
            return jsonify({"ok": False, "error": "Name and valid email required"})
        try:
            conn.execute("""
                INSERT INTO contacts
                  (name, email, first_seen, last_seen, email_count, topics,
                   company, how_met, relationship_tags, notes, contact_source)
                VALUES (?, ?, ?, ?, 0, '[]', ?, ?, '[]', '', 'manual')
                ON CONFLICT(email) DO UPDATE SET
                    name    = excluded.name,
                    company = COALESCE(NULLIF(excluded.company, ''), company),
                    how_met = COALESCE(NULLIF(excluded.how_met, ''), how_met)
            """, (name, email, now, now,
                  data.get("company", ""), data.get("how_met", "")))
            conn.commit()
            added = 1
        except Exception as e:
            errors.append(str(e))
        conn.close()
        return jsonify({"ok": True, "added": added, "errors": errors})

    # CSV upload
    file = request.files.get("file")
    if not file or not file.filename:
        conn.close()
        return jsonify({"ok": False, "error": "No file provided"})

    if not file.filename.lower().endswith(".csv"):
        conn.close()
        return jsonify({"ok": False, "error": "Please upload a .csv file"})

    try:
        raw = file.read()
        print(f"[Import] File: {file.filename}, size: {len(raw)} bytes")
        if not raw:
            conn.close()
            return jsonify({"ok": False, "error": "File is empty"})
        content = raw.decode("utf-8-sig")
        # Skip any leading blank lines (some LinkedIn exports have them)
        lines = content.strip().split("\n")
        # Find the header row (should contain "First Name" or "Name")
        header_idx = 0
        for i, line in enumerate(lines):
            if "First Name" in line or "Name" in line or "Email" in line:
                header_idx = i
                break
        content = "\n".join(lines[header_idx:])
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            # Handle LinkedIn and generic CSV column names
            first   = (row.get("First Name") or row.get("first_name") or "").strip()
            last    = (row.get("Last Name")  or row.get("last_name")  or "").strip()
            name    = f"{first} {last}".strip() or (row.get("Name") or row.get("name") or "").strip()
            email   = (row.get("Email Address") or row.get("Email") or row.get("email") or "").strip().lower()
            company = (row.get("Company") or row.get("Organization") or row.get("company") or "").strip()
            position = (row.get("Position") or row.get("Title") or row.get("title") or "").strip()
            linkedin_url = (row.get("URL") or row.get("url") or row.get("LinkedIn") or "").strip()
            connected_on = (row.get("Connected On") or row.get("connected_on") or "").strip()

            if not name:
                skipped += 1
                continue

            # Parse Connected On date (format: "01 Jan 2024" or similar)
            first_seen = now
            if connected_on:
                try:
                    parsed = datetime.strptime(connected_on, "%d %b %Y")
                    first_seen = parsed.replace(tzinfo=timezone.utc).isoformat()
                except Exception:
                    pass

            # If has email, use email as unique key
            if email and "@" in email:
                try:
                    conn.execute("""
                        INSERT INTO contacts
                          (name, email, first_seen, last_seen, email_count, topics,
                           company, how_met, relationship_tags, notes, linkedin, title, contact_source)
                        VALUES (?, ?, ?, ?, 0, '[]', ?, 'LinkedIn', '["LinkedIn"]', '', ?, ?, 'linkedin')
                        ON CONFLICT(email) DO UPDATE SET
                            name     = excluded.name,
                            company  = COALESCE(NULLIF(excluded.company, ''), company),
                            linkedin = COALESCE(NULLIF(excluded.linkedin, ''), linkedin),
                            title    = COALESCE(NULLIF(excluded.title, ''), title),
                            contact_source = CASE WHEN contacts.contact_source = 'gmail' THEN 'both' ELSE excluded.contact_source END
                    """, (name, email, first_seen, first_seen, company, linkedin_url, position))
                    added += 1
                except Exception as e:
                    errors.append(f"{name}: {e}")
            else:
                # No email — check for existing by linkedin URL or name+company
                existing = None
                if linkedin_url:
                    existing = conn.execute(
                        "SELECT id FROM contacts WHERE linkedin = ?", (linkedin_url,)
                    ).fetchone()
                if not existing:
                    existing = conn.execute(
                        "SELECT id FROM contacts WHERE name = ? AND company = ?", (name, company)
                    ).fetchone()

                if existing:
                    conn.execute("""
                        UPDATE contacts SET
                            linkedin = COALESCE(NULLIF(?, ''), linkedin),
                            title    = COALESCE(NULLIF(?, ''), title),
                            company  = COALESCE(NULLIF(?, ''), company),
                            contact_source = CASE WHEN contact_source = 'gmail' THEN 'both' ELSE 'linkedin' END
                        WHERE id = ?
                    """, (linkedin_url, position, company, existing["id"]))
                    added += 1
                else:
                    # Insert without email — use NULL email
                    try:
                        conn.execute("""
                            INSERT INTO contacts
                              (name, email, first_seen, last_seen, email_count, topics,
                               company, how_met, relationship_tags, notes, linkedin, title, contact_source)
                            VALUES (?, NULL, ?, ?, 0, '[]', ?, 'LinkedIn', '["LinkedIn"]', '', ?, ?, 'linkedin')
                        """, (name, first_seen, first_seen, company, linkedin_url, position))
                        added += 1
                    except Exception as e:
                        errors.append(f"{name}: {e}")
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"ok": False, "error": str(e)})

    conn.close()
    return jsonify({"ok": True, "added": added, "skipped": skipped, "errors": errors,
                     "total_rows": added + skipped + len(errors)})


DEFAULT_WARMTH_THRESHOLDS = {"healthy": 7, "warm": 30, "cold": 60}


def get_warmth_thresholds(conn=None):
    """Load user-configured warmth thresholds from settings, or use defaults."""
    try:
        if conn is None:
            conn = get_conn()
            row = conn.execute("SELECT value FROM settings WHERE key = 'warmth_thresholds'").fetchone()
            conn.close()
        else:
            row = conn.execute("SELECT value FROM settings WHERE key = 'warmth_thresholds'").fetchone()
        if row:
            val = row["value"] if hasattr(row, "keys") else row[0]
            if val:
                saved = json.loads(val)
                return {
                    "healthy": int(saved.get("healthy", 7)),
                    "warm": int(saved.get("warm", 30)),
                    "cold": int(saved.get("cold", 60)),
                }
    except Exception:
        pass
    return dict(DEFAULT_WARMTH_THRESHOLDS)


def compute_health(last_seen_str, reminder_date_str, thresholds=None):
    """Return health state: healthy, warm, cold, or dormant."""
    if thresholds is None:
        thresholds = DEFAULT_WARMTH_THRESHOLDS
    today = datetime.now(timezone.utc).date()

    # Check overdue follow-up first — forces cold
    if reminder_date_str:
        try:
            rd = datetime.fromisoformat(reminder_date_str).date()
            if rd < today:
                return "cold"
        except Exception:
            pass

    dt = parse_date(last_seen_str)
    if not dt:
        return "dormant"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    days = (datetime.now(timezone.utc) - dt).days
    if days <= thresholds["healthy"]:
        return "healthy"
    elif days <= thresholds["warm"]:
        return "warm"
    elif days <= thresholds["cold"]:
        return "cold"
    return "dormant"


HEALTH_COLORS = {
    "healthy": "#1D9E75",
    "warm":    "#BA7517",
    "cold":    "#E24B4A",
    "dormant": "#444",
}


# ── Auto-sync state ──
_sync_lock = threading.Lock()
_sync_running = False


def _run_sync():
    global _sync_running
    try:
        token_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
        if not os.path.exists(token_file):
            print("[sync] Skipped — token.json not found. Run gmail_auth.py first.")
            return
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contacts_extract.py")
        subprocess.run(["python3", script], env=os.environ.copy(), timeout=1800)
        conn = sqlite3.connect(DB_FILE)
        conn.execute(
            "INSERT INTO settings (key, value) VALUES ('last_synced_at', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (datetime.now(timezone.utc).isoformat(),)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[sync] Error: {e}")
    finally:
        with _sync_lock:
            _sync_running = False


def _gmail_connected():
    """Check if OAuth token file exists (Gmail setup is complete)."""
    token_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
    return os.path.exists(token_file)


def maybe_auto_sync():
    """Trigger background sync if 6+ hours since last sync."""
    global _sync_running
    if not _gmail_connected():
        return
    with _sync_lock:
        if _sync_running:
            return
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        row = conn.execute("SELECT value FROM settings WHERE key = 'last_synced_at'").fetchone()
        conn.close()

        should_sync = True
        if row and row[0]:
            try:
                last = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
                if not last.tzinfo:
                    last = last.replace(tzinfo=timezone.utc)
                hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
                should_sync = hours >= 6
            except Exception:
                pass

        if should_sync:
            with _sync_lock:
                _sync_running = True
            t = threading.Thread(target=_run_sync, daemon=True)
            t.start()
    except Exception:
        pass


def palette_name(name):
    h = int(hashlib.md5((name or "").encode()).hexdigest(), 16)
    return PALETTE_NAMES[h % len(PALETTE_NAMES)]


def compact_date(date_str):
    """Return (label, staleness) with compact labels like '1d ago', '2w ago'."""
    dt = parse_date(date_str)
    if not dt:
        return ("—", "recent")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - dt
    days = delta.days
    hours = delta.seconds // 3600
    if days == 0:
        label = "now" if hours == 0 else f"{hours}h ago"
        return (label, "recent")
    elif days == 1:
        return ("1d ago", "recent")
    elif days <= 6:
        return (f"{days}d ago", "stale" if days >= 4 else "recent")
    elif days <= 27:
        weeks = days // 7
        return (f"{weeks}w ago", "stale")
    else:
        months = days // 30
        return (f"{months}mo ago" if months >= 1 else f"{days}d ago", "stale")


@app.route("/contacts-view")
def contacts_view():
    # Trigger auto-sync if needed
    maybe_auto_sync()

    show = request.args.get("show", "active")
    conn = get_conn()
    if show == "archived":
        where = "WHERE archived = 1"
    elif show == "all":
        where = ""
    else:
        where = "WHERE archived = 0"
    rows = conn.execute(
        "SELECT id, name, email, last_seen, email_count, topics, "
        "company, reminder_date, relationship_tags, how_met, notes, last_gmail_id, last_received_date, "
        "birthday, archived, contact_source, linkedin "
        f"FROM contacts {where} ORDER BY last_seen DESC"
    ).fetchall()

    contacts = []
    all_tags_set = set()
    today = datetime.now(timezone.utc).date()
    now = datetime.now(timezone.utc)
    warmth_thresholds = get_warmth_thresholds(conn)

    for r in rows:
        name = r["name"] or ""
        email = r["email"] or ""
        company = r["company"] or ""
        how_met = r["how_met"] or ""
        notes = r["notes"] or ""
        tags = json.loads(r["relationship_tags"] or "[]")
        for t in tags:
            all_tags_set.add(t)

        # Last contact
        label, staleness = compact_date(r["last_seen"])

        # Thread URL from last_gmail_id (stored by contacts_extract)
        last_gmail_id = None
        try:
            last_gmail_id = r["last_gmail_id"]
        except (IndexError, KeyError):
            pass
        thread_url = f"https://mail.google.com/mail/u/0/#inbox/{last_gmail_id}" if last_gmail_id else ""

        # Last thread subject from topics
        last_thread = ""
        topics_list = json.loads(r["topics"] or "[]")
        if topics_list:
            last_thread = topics_list[-1]

        # Needs-reply detection using last_received_date
        # If last_seen ≈ last_received_date → user hasn't replied since receiving
        needs_reply = False
        received_ago_days = None

        received_date_str = None
        try:
            received_date_str = r["last_received_date"]
        except (IndexError, KeyError):
            pass

        if received_date_str:
            received_dt = parse_date(received_date_str)
            last_seen_dt = parse_date(r["last_seen"])
            if received_dt and last_seen_dt:
                if received_dt.tzinfo is None:
                    received_dt = received_dt.replace(tzinfo=timezone.utc)
                if last_seen_dt.tzinfo is None:
                    last_seen_dt = last_seen_dt.replace(tzinfo=timezone.utc)
                gap = abs((last_seen_dt - received_dt).total_seconds())
                if gap < 7200:  # within 2 hours = user hasn't replied
                    received_ago_days = (now - received_dt).days
                    if received_ago_days >= 1:
                        needs_reply = True

        # Follow-up badge
        badge_type = "none"
        badge_label = ""
        reminder_date = r["reminder_date"]
        if needs_reply:
            badge_type = "needs-reply"
            if received_ago_days and received_ago_days > 3:
                badge_label = f"Reply — {received_ago_days}d ago"
            else:
                badge_label = "Needs reply"
        if reminder_date:
            try:
                rd = datetime.fromisoformat(reminder_date).date()
                diff = (rd - today).days
                if diff < 0:
                    badge_type = "overdue"
                    badge_label = "Overdue" if diff == -1 else f"{abs(diff)}d overdue"
                elif diff == 0:
                    badge_type = "soon"
                    badge_label = "Today"
                elif diff == 1:
                    badge_type = "soon"
                    badge_label = "Tomorrow"
                elif diff <= 7:
                    badge_type = "soon"
                    badge_label = f"In {diff}d"
            except Exception:
                pass

        # Health
        health = compute_health(r["last_seen"], reminder_date, warmth_thresholds)

        # Birthday
        birthday = ""
        try:
            birthday = r["birthday"] or ""
        except (IndexError, KeyError):
            pass

        archived = 0
        try:
            archived = r["archived"] or 0
        except (IndexError, KeyError):
            pass

        source = "gmail"
        try:
            source = r["contact_source"] or "gmail"
        except (IndexError, KeyError):
            pass

        linkedin_url = ""
        try:
            linkedin_url = r["linkedin"] or ""
        except (IndexError, KeyError):
            pass

        c = {
            "id":             r["id"],
            "name":           name,
            "initials":       initials(name),
            "email":          email,
            "company":        company,
            "how_met":        how_met,
            "notes":          notes,
            "palette":        palette_name(name),
            "last_contact":   label,
            "staleness":      staleness,
            "last_thread":    last_thread[:50],
            "thread_url":     thread_url,
            "badge_type":     badge_type,
            "badge_label":    badge_label,
            "needs_reply":    needs_reply,
            "tags":           tags,
            "tags_lower":     " ".join(t.lower() for t in tags),
            "reminder_date":  reminder_date or "",
            "last_seen_raw":  r["last_seen"] or "",
            "health":         health,
            "health_color":   HEALTH_COLORS[health],
            "health_label":   health.capitalize(),
            "birthday":       birthday,
            "archived":       archived,
            "source":         source,
            "linkedin_url":   linkedin_url,
        }
        contacts.append(c)

    # Collect known tags for filter pills
    for default_tag in ["Investor", "Advisor", "Classmate", "Founder", "Recruiter", "Other"]:
        all_tags_set.add(default_tag)
    all_tags = sorted(all_tags_set)

    # Pagination
    page = request.args.get("page", 1, type=int)
    per_page = 100
    total_contacts = len(contacts)
    total_pages = max(1, (total_contacts + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    paginated = contacts[(page - 1) * per_page : page * per_page]

    # Last synced
    sync_row = conn.execute("SELECT value FROM settings WHERE key = 'last_synced_at'").fetchone()
    last_synced = relative_sync(sync_row["value"] if sync_row else None)

    conn.close()
    return render_template("contacts.html",
                           contacts=paginated,
                           total_contacts=total_contacts,
                           all_tags=all_tags,
                           last_synced=last_synced,
                           sync_running=_sync_running,
                           show=show,
                           page=page,
                           total_pages=total_pages,
                           gmail_connected=_gmail_connected(),
                           warmth_thresholds=warmth_thresholds)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() in ("1", "true")
    app.run(debug=debug, port=port)
