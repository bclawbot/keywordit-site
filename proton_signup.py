#!/usr/bin/env python3
"""
proton_signup.py — Create a Proton Mail (Free) account using a real browser.

headless=False lets Chrome's real fingerprint pass hCaptcha.
A Chrome window appears briefly (~20-40s). This is expected.

Requires: playwright (pip install playwright && playwright install chromium)
"""

import asyncio
import random
import string
import sys
import json
from pathlib import Path

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
    from playwright._impl._errors import TargetClosedError
except ImportError:
    print("[!] playwright not installed. Run: pip install playwright && playwright install chromium", file=sys.stderr)
    sys.exit(1)


def random_username(length=10):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def strong_password(length=16):
    chars = string.ascii_letters + string.digits + "!@#$%"
    return "".join(random.choices(chars, k=length))


SIGNUP_URL = "https://account.proton.me/signup?plan=free"
SCREENSHOT_DIR = Path("/tmp")


async def safe_screenshot(page, label):
    try:
        path = str(SCREENSHOT_DIR / f"proton_{label}.png")
        await page.screenshot(path=path)
        print(f"[*] Screenshot: {path}")
    except Exception:
        pass


async def try_fill_input(frame_or_page, selector, value, label=""):
    """Try to fill an input. Returns True on success."""
    try:
        el = frame_or_page.locator(selector).first
        if await el.is_visible(timeout=1500):
            await el.fill(value)
            print(f"[+] Filled {label or selector}")
            return True
    except (PlaywrightTimeout, TargetClosedError, Exception):
        pass
    return False


async def signup():
    username = random_username()
    password = strong_password()

    print(f"[*] Target: {username}@proton.me")
    print(f"[*] Password: {password}")
    print("[*] Launching real Chrome (headless=False)...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # ── Navigate ──────────────────────────────────────────────────────────
        print(f"[*] Navigating to {SIGNUP_URL}")
        await page.goto(SIGNUP_URL, wait_until="domcontentloaded", timeout=30000)
        # Wait for challenge iframes to load (up to 8s)
        await page.wait_for_timeout(8000)
        await safe_screenshot(page, "01_loaded")

        # ── Debug: list all frames ────────────────────────────────────────────
        print(f"[*] Frames loaded: {len(page.frames)}")
        for i, frame in enumerate(page.frames):
            print(f"    [{i}] {frame.url[:80]}")

        # ── Fill username ─────────────────────────────────────────────────────
        # Proton puts the username input in a sandboxed iframe ("challenge" iframe).
        # The iframe URL contains "challenge" but the exact path varies.
        # We try every non-main frame first, then fall back to main frame.
        filled_user = False

        USERNAME_SELECTORS = [
            'input[id="email"]',
            'input[id="username"]',
            'input[name="username"]',
            'input[name="email"]',
            'input[autocomplete="username"]',
            'input[type="text"]',
            'input:not([type="password"]):not([type="hidden"]):not([type="submit"])',
        ]

        # Pass 1: check all iframes
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            if "challenge" not in frame.url and "signup" not in frame.url:
                continue
            for sel in USERNAME_SELECTORS:
                if await try_fill_input(frame, sel, username, f"username (iframe/{sel})"):
                    filled_user = True
                    break
            if filled_user:
                break

        # Pass 2: try all iframes regardless of URL
        if not filled_user:
            for frame in page.frames:
                if frame == page.main_frame:
                    continue
                for sel in USERNAME_SELECTORS:
                    if await try_fill_input(frame, sel, username, f"username (any-iframe/{sel})"):
                        filled_user = True
                        break
                if filled_user:
                    break

        # Pass 3: main frame
        if not filled_user:
            for sel in USERNAME_SELECTORS:
                if await try_fill_input(page, sel, username, f"username (main/{sel})"):
                    filled_user = True
                    break

        if not filled_user:
            await safe_screenshot(page, "ERR_no_username")
            print(f"[!] Username field not found. URL: {page.url}", file=sys.stderr)
            print("[!] See /tmp/proton_ERR_no_username.png", file=sys.stderr)
            await browser.close()
            return None

        await page.wait_for_timeout(500)

        # ── Fill password ─────────────────────────────────────────────────────
        # Fill first password field
        for sel in ['input[id="password"]', 'input[name="password"]']:
            if await try_fill_input(page, sel, password, f"password ({sel})"):
                break

        await page.wait_for_timeout(700)  # let confirm field appear

        # Fill confirm password — separate field with its own selector
        confirm_filled = False
        for sel in [
            'input[id="repeat-password"]',
            'input[id="password-repeat"]',
            'input[id="confirmPassword"]',
            'input[name="confirmPassword"]',
            'input[name="confirm-password"]',
            'input[placeholder*="onfirm" i]',
        ]:
            if await try_fill_input(page, sel, password, f"confirm password ({sel})"):
                confirm_filled = True
                break

        # Fallback: if 2 password-type inputs exist, fill the second one
        if not confirm_filled:
            try:
                els = page.locator('input[type="password"]')
                count = await els.count()
                if count >= 2:
                    await els.nth(1).fill(password)
                    print("[+] Confirm password filled (input[type=password]:nth(1))")
                    confirm_filled = True
            except (PlaywrightTimeout, TargetClosedError):
                pass

        if not confirm_filled:
            print("[!] Confirm password field not found — submit may fail validation", file=sys.stderr)

        await page.wait_for_timeout(500)
        await safe_screenshot(page, "02_filled")

        # ── Submit ────────────────────────────────────────────────────────────
        SUBMIT_SELECTORS = [
            'button[type="submit"]',
            'button:has-text("Start using Proton Mail")',
            'button:has-text("Create account")',
            'button:has-text("Continue")',
        ]
        for sel in SUBMIT_SELECTORS:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    print(f"[+] Clicked submit ({sel})")
                    break
            except (PlaywrightTimeout, TargetClosedError):
                pass

        # ── Post-submit: handle each interstitial page sequentially ──────────
        print("[*] Handling post-submit flow...")
        success = False
        captcha_appeared = False

        async def current_url():
            try:
                return page.url
            except Exception:
                return ""

        async def is_success_url():
            u = await current_url()
            return any(x in u for x in ["/mail/", "/calendar/", "/drive/", "mail.proton.me"])

        async def dismiss_upsell():
            """Close the upsell modal. Returns True if modal was found and closed."""
            upsell_visible = False
            try:
                upsell_visible = await page.get_by_text("Get limited-time offer", exact=False).is_visible(timeout=500)
            except (PlaywrightTimeout, TargetClosedError):
                pass
            if not upsell_visible:
                return False

            print("[*] Upsell modal detected — closing...")

            # Strategy A: click the × button in the modal header
            # Proton's modal header has a button with an svg cross icon
            for sel in [
                'button[aria-label="Close"]',
                'button[aria-label="close"]',
                'button[data-testid*="close" i]',
                '.modal-header button',
                '[role="dialog"] button:first-child',
                '[role="dialog"] header button',
                'dialog button',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=300):
                        await btn.click(force=True)
                        print(f"[+] Closed modal via: {sel}")
                        await page.wait_for_timeout(1500)
                        return True
                except (PlaywrightTimeout, TargetClosedError):
                    pass

            # Strategy B: click the visible × at the top-right of the modal by bounding box
            try:
                modal = page.locator('[role="dialog"], .modal, [class*="Modal"]').first
                if await modal.is_visible(timeout=300):
                    box = await modal.bounding_box()
                    if box:
                        # × is near top-right corner
                        x = box["x"] + box["width"] - 20
                        y = box["y"] + 20
                        await page.mouse.click(x, y)
                        print(f"[+] Clicked modal close by position ({x:.0f}, {y:.0f})")
                        await page.wait_for_timeout(1500)
                        return True
            except (PlaywrightTimeout, TargetClosedError):
                pass

            # Strategy C: Escape key
            try:
                await page.keyboard.press("Escape")
                print("[+] Sent Escape to close modal")
                await page.wait_for_timeout(1500)
                return True
            except (PlaywrightTimeout, TargetClosedError):
                pass

            return False

        async def handle_recovery_kit():
            """Check checkbox + click Continue on the recovery kit page."""
            try:
                heading = page.get_by_text("Secure your account", exact=False)
                if not await heading.is_visible(timeout=800):
                    return False
            except (PlaywrightTimeout, TargetClosedError):
                return False

            print("[*] Recovery kit page — accepting...")
            await safe_screenshot(page, "04_recovery_kit")
            for cb_sel in ['input[type="checkbox"]', '[role="checkbox"]']:
                try:
                    cb = page.locator(cb_sel).first
                    if await cb.is_visible(timeout=1500):
                        await cb.click()
                        print("[+] Checked 'I understand'")
                        break
                except (PlaywrightTimeout, TargetClosedError):
                    pass
            await page.wait_for_timeout(500)
            for btn_text in ["Continue", "Next", "Done"]:
                try:
                    btn = page.get_by_role("button", name=btn_text, exact=False)
                    if await btn.is_visible(timeout=1500):
                        await btn.click()
                        print(f"[+] Clicked '{btn_text}'")
                        await page.wait_for_timeout(3000)
                        return True
                except (PlaywrightTimeout, TargetClosedError):
                    pass
            return False

        # Poll for up to 90s, taking a screenshot every 15 ticks for debugging
        for tick in range(90):
            try:
                if await is_success_url():
                    success = True
                    break

                if await dismiss_upsell():
                    await safe_screenshot(page, f"05_after_upsell_{tick}")
                    # After closing upsell, session cookies should be set.
                    # Navigate to mail.proton.me as definitive auth check.
                    try:
                        await page.goto("https://mail.proton.me", wait_until="domcontentloaded", timeout=20000)
                        await page.wait_for_timeout(3000)
                        await safe_screenshot(page, f"06_mail_check_{tick}")
                        if "mail.proton.me" in page.url and "login" not in page.url:
                            print("[+] Authenticated at mail.proton.me!")
                            success = True
                            break
                        else:
                            print(f"[*] mail.proton.me → {page.url} (not authenticated)")
                    except (PlaywrightTimeout, TargetClosedError):
                        pass

                if await handle_recovery_kit():
                    await safe_screenshot(page, f"07_after_recovery_{tick}")

                if await is_success_url():
                    success = True
                    break

                # Detect hCaptcha
                for frame in [page] + list(page.frames):
                    try:
                        for sel in ['iframe[src*="hcaptcha"]', '[id*="captcha"]']:
                            if await frame.locator(sel).first.is_visible(timeout=200):
                                captcha_appeared = True
                    except Exception:
                        pass
                if captcha_appeared:
                    break

                if tick % 15 == 14:
                    await safe_screenshot(page, f"loop_{tick+1}s")

                await page.wait_for_timeout(1000)
            except TargetClosedError:
                print(f"[!] Browser closed at tick {tick}")
                break

        await safe_screenshot(page, "03_result")
        final_url = page.url
        await browser.close()

        if success:
            result = {"address": f"{username}@proton.me", "password": password}
            print("\n=== Proton Account Created ===")
            print(f"  Address  : {result['address']}")
            print(f"  Password : {result['password']}")
            print(f"\n{json.dumps(result, indent=2)}")
            return result
        elif captcha_appeared:
            print("[!] hCaptcha appeared.", file=sys.stderr)
            print("[!] Use mailtm_signup.py for guaranteed zero-captcha creation.", file=sys.stderr)
            return None
        else:
            print(f"[!] Signup did not complete. Final URL: {final_url}", file=sys.stderr)
            print("[!] Check /tmp/proton_*.png for debug screenshots.", file=sys.stderr)
            return None


def main():
    result = asyncio.run(signup())
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
