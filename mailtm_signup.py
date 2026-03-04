#!/usr/bin/env python3
"""
mailtm_signup.py — Create a free disposable email account via Mail.tm REST API.

No captcha. No paid APIs. Uses only httpx (already installed).
Prints: address, password, JWT token (for reading inbox later).
"""

import httpx
import random
import string
import json
import sys

BASE = "https://api.mail.tm"


def random_string(length=10):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def strong_password(length=16):
    chars = string.ascii_letters + string.digits + "!@#$%"
    return "".join(random.choices(chars, k=length))


def get_domain(client):
    r = client.get(f"{BASE}/domains")
    r.raise_for_status()
    data = r.json()
    domains = data.get("hydra:member", data) if isinstance(data, dict) else data
    if not domains:
        raise RuntimeError("No domains available from Mail.tm")
    return domains[0]["domain"]


def create_account(client, address, password):
    r = client.post(f"{BASE}/accounts", json={"address": address, "password": password})
    if r.status_code == 422:
        detail = r.json()
        raise RuntimeError(f"Account creation failed (422): {detail}")
    r.raise_for_status()
    return r.json()


def get_token(client, address, password):
    r = client.post(f"{BASE}/token", json={"address": address, "password": password})
    r.raise_for_status()
    return r.json()["token"]


def main():
    with httpx.Client(timeout=15) as client:
        print("[*] Fetching available domains...")
        domain = get_domain(client)
        print(f"[*] Domain: {domain}")

        username = random_string(10)
        address = f"{username}@{domain}"
        password = strong_password()

        print(f"[*] Creating account: {address}")
        account = create_account(client, address, password)
        account_id = account.get("id", "unknown")
        print(f"[+] Account created (id: {account_id})")

        print("[*] Getting JWT token...")
        token = get_token(client, address, password)

        result = {
            "address": address,
            "password": password,
            "token": token,
            "id": account_id,
        }

        print("\n=== Email Account Created ===")
        print(f"  Address  : {address}")
        print(f"  Password : {password}")
        print(f"  Token    : {token[:40]}...")
        print(f"\n[*] Full JSON:\n{json.dumps(result, indent=2)}")
        return result


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[!] Error: {e}", file=sys.stderr)
        sys.exit(1)
