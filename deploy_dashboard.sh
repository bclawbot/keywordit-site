#!/usr/bin/env bash
# deploy_dashboard.sh — Push dashboard.html to the gh-pages branch for keywordit.xyz
# Called as Stage 6 by heartbeat.py after dashboard_builder.py succeeds.
# Safe to run standalone. Exits 0 (non-fatal) if no remote is configured.

set -euo pipefail

WORKSPACE="$HOME/.openclaw/workspace"
DASHBOARD="$WORKSPACE/dashboard.html"

# ── Pre-flight checks ──────────────────────────────────────────────────────────

cd "$WORKSPACE"

# Bail gracefully if no git remote — won't block the pipeline
if ! git remote get-url origin >/dev/null 2>&1; then
    echo "[deploy] No git remote configured — skipping GitHub Pages deploy"
    echo "[deploy] To enable: cd ~/.openclaw/workspace && git remote add origin https://github.com/<user>/<repo>.git"
    exit 0
fi

if [ ! -f "$DASHBOARD" ]; then
    echo "[deploy] dashboard.html not found — skipping deploy"
    exit 0
fi

# ── Worktree deploy (no branch switching in main tree) ────────────────────────

DEPLOY_DIR=$(mktemp -d)
cleanup() { git worktree remove "$DEPLOY_DIR" --force 2>/dev/null || true; rm -rf "$DEPLOY_DIR"; }
trap cleanup EXIT

# Fetch latest gh-pages (or create orphan if it doesn't exist yet)
git fetch origin gh-pages --quiet 2>/dev/null || true

if git show-ref --verify --quiet refs/remotes/origin/gh-pages; then
    git worktree add "$DEPLOY_DIR" origin/gh-pages --quiet 2>/dev/null || \
      git worktree add "$DEPLOY_DIR" --detach --quiet
    # Make sure local gh-pages branch tracks remote
    if ! git show-ref --verify --quiet refs/heads/gh-pages; then
        git branch gh-pages origin/gh-pages --quiet
    fi
    git -C "$DEPLOY_DIR" checkout gh-pages --quiet 2>/dev/null || true
else
    # First deploy — create orphan gh-pages
    echo "[deploy] Creating gh-pages branch for the first time"
    git worktree add "$DEPLOY_DIR" --orphan gh-pages --quiet
fi

# ── Copy files ────────────────────────────────────────────────────────────────

cp "$DASHBOARD" "$DEPLOY_DIR/index.html"
[ -f "$WORKSPACE/logo-dark.png" ]  && cp "$WORKSPACE/logo-dark.png"  "$DEPLOY_DIR/"
[ -f "$WORKSPACE/logo-light.png" ] && cp "$WORKSPACE/logo-light.png" "$DEPLOY_DIR/"
[ -f "$WORKSPACE/landing.html" ]   && cp "$WORKSPACE/landing.html"   "$DEPLOY_DIR/"
echo "keywordit.xyz" > "$DEPLOY_DIR/CNAME"

# ── Commit and push ───────────────────────────────────────────────────────────

cd "$DEPLOY_DIR"
git add -A

if git diff --cached --quiet; then
    echo "[deploy] No changes since last deploy — skipping push"
    exit 0
fi

TIMESTAMP=$(date +"%Y-%m-%d %H:%M")
git commit -m "deploy: dashboard update $TIMESTAMP" --quiet
git push -f origin gh-pages --quiet

echo "[deploy] ✅ Deployed dashboard.html → gh-pages ($TIMESTAMP)"
