#!/usr/bin/env python3
"""
google_ads_setup.py — One-time OAuth2 credential helper for Google Ads API.

Run this ONCE to generate your refresh token. After that, add the printed
credentials to ~/.openclaw/.env and never run this again.

Requirements:
  pip install google-ads google-auth-oauthlib

Usage:
  1. Set GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET in your shell:
       export GOOGLE_ADS_CLIENT_ID=your-client-id.apps.googleusercontent.com
       export GOOGLE_ADS_CLIENT_SECRET=your-client-secret
  2. Run: python3 scripts/google_ads_setup.py
  3. Authorize in browser, paste the code back here.
  4. Copy the printed credentials into ~/.openclaw/.env
"""

import json
import os
import sys
import webbrowser
from pathlib import Path

ENV_FILE = Path.home() / ".openclaw" / ".env"

# ── Load .env for credentials ─────────────────────────────────────────────────

def _load_env(path: Path) -> dict:
    env = {}
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return env

_env = _load_env(ENV_FILE)

CLIENT_ID     = os.environ.get("GOOGLE_ADS_CLIENT_ID")     or _env.get("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or _env.get("GOOGLE_ADS_CLIENT_SECRET", "")

if not CLIENT_ID or not CLIENT_SECRET:
    print("❌  GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET must be set.")
    print()
    print("How to get them:")
    print("  1. Go to https://console.cloud.google.com/apis/credentials")
    print("  2. Create an OAuth2 client ID (Application type: Desktop app)")
    print("  3. Enable 'Google Ads API' in the API Library")
    print("  4. Export the values:")
    print("       export GOOGLE_ADS_CLIENT_ID=xxx.apps.googleusercontent.com")
    print("       export GOOGLE_ADS_CLIENT_SECRET=xxx")
    print("  5. Re-run this script.")
    sys.exit(1)

# ── OAuth2 flow ───────────────────────────────────────────────────────────────

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("❌  google-auth-oauthlib not found. Run: pip install google-auth-oauthlib")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/adwords"]

client_config = {
    "installed": {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
        "token_uri":     "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost:8080", "http://localhost"],
    }
}

print("=" * 60)
print("  Google Ads API — OAuth2 Setup")
print("=" * 60)
print()
print("Step 1: Starting local OAuth server on http://localhost:8080 ...")
print("        (Google Cloud Console must have http://localhost:8080 in Authorized redirect URIs,")
print("         OR your OAuth client type must be 'Desktop app')")

flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)

print("Step 2: A browser window will open. Authorize access and wait for the")
print("        'Authentication complete' page — then return here.")
print()

try:
    credentials = flow.run_local_server(
        port=8080,
        access_type="offline",
        prompt="consent",
        open_browser=True,
    )
except Exception as e:
    print(f"❌  OAuth flow failed: {e}")
    sys.exit(1)
refresh_token = credentials.refresh_token

if not refresh_token:
    print("❌  No refresh token returned. Make sure you requested offline access.")
    sys.exit(1)

# ── Validate with a test API call ─────────────────────────────────────────────

DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or _env.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CUSTOMER_ID     = os.environ.get("GOOGLE_ADS_CUSTOMER_ID")     or _env.get("GOOGLE_ADS_CUSTOMER_ID", "")

print()
print("=" * 60)
print("  Credentials generated successfully!")
print("=" * 60)
print()
print("Add these to ~/.openclaw/.env:")
print()
print(f"GOOGLE_ADS_CLIENT_ID={CLIENT_ID}")
print(f"GOOGLE_ADS_CLIENT_SECRET={CLIENT_SECRET}")
print(f"GOOGLE_ADS_REFRESH_TOKEN={refresh_token}")
if DEVELOPER_TOKEN:
    print(f"GOOGLE_ADS_DEVELOPER_TOKEN={DEVELOPER_TOKEN}  # already set")
else:
    print("GOOGLE_ADS_DEVELOPER_TOKEN=<get from https://ads.google.com/aw/apicenter>")
if CUSTOMER_ID:
    print(f"GOOGLE_ADS_CUSTOMER_ID={CUSTOMER_ID}  # already set")
else:
    print("GOOGLE_ADS_CUSTOMER_ID=<your 10-digit account ID, no dashes>")
print()

if DEVELOPER_TOKEN and CUSTOMER_ID:
    print("Validating credentials with a test API call...")
    try:
        from google.ads.googleads.client import GoogleAdsClient
        client = GoogleAdsClient.load_from_dict({
            "developer_token":   DEVELOPER_TOKEN,
            "client_id":         CLIENT_ID,
            "client_secret":     CLIENT_SECRET,
            "refresh_token":     refresh_token,
            "use_proto_plus":    True,
        })
        svc = client.get_service("KeywordPlanIdeaService")
        print("✅  Credentials valid — KeywordPlanIdeaService accessible.")
    except Exception as e:
        print(f"⚠️   Validation call failed (credentials may still be correct): {e}")
else:
    print("ℹ️   Set GOOGLE_ADS_DEVELOPER_TOKEN and GOOGLE_ADS_CUSTOMER_ID to validate.")

print()
print("Done. Run the pipeline after updating ~/.openclaw/.env.")
