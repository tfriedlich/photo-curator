"""
Run this once to get your Google Photos OAuth token.
Requires: pip install google-auth-oauthlib google-auth-httplib2

Usage:
    python google_auth.py

Then copy the printed token into your Railway GOOGLE_PHOTOS_TOKEN env var.
"""

import json
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
]

CREDS_FILE = "google_credentials.json"
TOKEN_FILE = "google_token.pickle"


def get_token():
    creds = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDS_FILE):
                print(f"\n❌ Missing {CREDS_FILE}")
                print("Download your OAuth credentials from Google Cloud Console:")
                print("  APIs & Services → Credentials → Create OAuth 2.0 Client ID (Desktop)")
                print(f"  Save as: {CREDS_FILE}\n")
                return

            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)

    print("\n✅ Access token (add to Railway as GOOGLE_PHOTOS_TOKEN):")
    print(f"\n{creds.token}\n")
    print("⚠️  This token expires in ~1 hour.")
    print("   Re-run this script before each trip to refresh it.")
    print(f"   Refresh token saved in {TOKEN_FILE} for future use.\n")


if __name__ == "__main__":
    get_token()
