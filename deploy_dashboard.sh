#!/usr/bin/env bash
# deploy_dashboard.sh — Auto-deploy dashboard to main + gh-pages
# Called as Stage 6 by heartbeat.py after dashboard_builder.py succeeds.
# Safe to run standalone. Exits 0 (non-fatal) if no remote is configured.

set -euo pipefail

WORKSPACE="$HOME/.openclaw/workspace"
DASHBOARD="$WORKSPACE/dashboard.html"

# ── Pre-flight checks ──────────────────────────────────────────────────────────

cd "$WORKSPACE"

# Bail gracefully if no git remote — won't block the pipeline
if ! git remote get-url origin >/dev/null 2>&1; then
    echo "[deploy] No git remote configured — skipping deploy"
    exit 0
fi

if [ ! -f "$DASHBOARD" ]; then
    echo "[deploy] dashboard.html not found — skipping deploy"
    exit 0
fi

# ── Deploy to main branch (for Netlify) ───────────────────────────────────────

echo "[deploy] Committing dashboard to main branch..."
git add dashboard.html
cp dashboard.html index.html
git add index.html

if ! git diff --cached --quiet; then
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M")
    git commit -m "auto: dashboard update $TIMESTAMP" --quiet
    git push origin main --quiet
    echo "[deploy] ✅ Pushed to main → Netlify will deploy"
else
    echo "[deploy] No changes to dashboard — skipping main push"
fi

# ── Deploy to gh-pages (GitHub Pages fallback) ───────────────────────────────

echo "[deploy] Deploying to gh-pages branch..."

# Use worktree to avoid switching branches in main workspace
DEPLOY_DIR=$(mktemp -d)
cleanup() { git worktree remove "$DEPLOY_DIR" --force 2>/dev/null || true; rm -rf "$DEPLOY_DIR"; }
trap cleanup EXIT

git fetch origin gh-pages --quiet 2>/dev/null || true

if git show-ref --verify --quiet refs/remotes/origin/gh-pages; then
    git worktree add "$DEPLOY_DIR" origin/gh-pages --quiet 2>/dev/null || \
      git worktree add "$DEPLOY_DIR" --detach --quiet
    if ! git show-ref --verify --quiet refs/heads/gh-pages; then
        git branch gh-pages origin/gh-pages --quiet
    fi
    git -C "$DEPLOY_DIR" checkout gh-pages --quiet 2>/dev/null || true
else
    echo "[deploy] Creating gh-pages branch for the first time"
    git worktree add "$DEPLOY_DIR" --orphan gh-pages --quiet
fi

# Copy files to gh-pages worktree
cp "$DASHBOARD" "$DEPLOY_DIR/index.html"
[ -f "$WORKSPACE/logo-dark.png" ]  && cp "$WORKSPACE/logo-dark.png"  "$DEPLOY_DIR/"
[ -f "$WORKSPACE/logo-light.png" ] && cp "$WORKSPACE/logo-light.png" "$DEPLOY_DIR/"
[ -f "$WORKSPACE/landing.html" ]   && cp "$WORKSPACE/landing.html"   "$DEPLOY_DIR/"
[ -f "$WORKSPACE/login.html" ]     && cp "$WORKSPACE/login.html"     "$DEPLOY_DIR/"
[ -f "$WORKSPACE/admin.html" ]     && cp "$WORKSPACE/admin.html"     "$DEPLOY_DIR/"
echo "keywordit.xyz" > "$DEPLOY_DIR/CNAME"

cd "$DEPLOY_DIR"
git add -A

if git diff --cached --quiet; then
    echo "[deploy] No changes to gh-pages — skipping push"
else
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M")
    git commit -m "deploy: dashboard update $TIMESTAMP" --quiet
    git push -f origin gh-pages --quiet
    echo "[deploy] ✅ Pushed to gh-pages"
fi

echo "[deploy] ✅ Deployment complete: main (Netlify) + gh-pages (GitHub Pages)"
