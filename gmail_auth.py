"""
gmail_auth.py - Gmail OAuth2 authentication

Run once to authorize. Saves token to token.json.
On subsequent runs, loads and auto-refreshes the token.

Requires GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in .env.
See README.md for setup instructions.
"""

import os
import sys

from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_FILE = "token.json"


def _get_client_config():
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        print(
            "ERROR: Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET.\n"
            "Set them in your .env file. See README.md for setup instructions.",
            file=sys.stderr,
        )
        sys.exit(1)
    return {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }


def authenticate():
    creds = None

    # Load existing token if available
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # Refresh or run full OAuth flow if needed
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired token...")
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Token refresh failed ({e}). Re-authenticating...")
                creds = None

        if not creds:
            client_config = _get_client_config()
            print("Opening browser for Gmail authorization...")
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

        # Persist token for future runs
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    print("Authentication successful.")
    return creds


if __name__ == "__main__":
    authenticate()
