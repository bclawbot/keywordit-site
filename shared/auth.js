/**
 * shared/auth.js — Keywordit session management (JWT backend)
 *
 * Usage:
 *   requireAuth()                       — redirect if not logged in
 *   requireAuth({ requireAdmin: true }) — redirect if not admin
 *   signOut()                           — clear session, go to login
 *   checkSession()                      — returns { valid, user, role, name } or { valid: false }
 *   auth.getHeaders()                   — returns { Authorization: 'Bearer ...' }
 *   auth.login(username, password)      — POST /api/auth/login
 *   auth.register(data)                 — POST /api/auth/register
 */
(function() {
  'use strict';

  // F-013: purge legacy leaked user data from returning visitors. Remove after 2026-10-22.
  try { localStorage.removeItem('keywordit_users'); } catch(e) {}

  var API_BASE = 'https://keywordit-api-production.up.railway.app';
  var SESSION_KEY = 'keywordit_session';
  var EXPIRY_MS = 24 * 60 * 60 * 1000;

  function getSession() {
    try { return JSON.parse(localStorage.getItem(SESSION_KEY) || 'null'); }
    catch(e) { return null; }
  }

  function saveSession(data) {
    localStorage.setItem(SESSION_KEY, JSON.stringify(data));
  }

  var auth = {
    apiBase: API_BASE,

    getHeaders: function() {
      var sess = getSession();
      var h = { 'Content-Type': 'application/json' };
      if (sess && sess.token) h['Authorization'] = 'Bearer ' + sess.token;
      return h;
    },

    login: async function(username, password) {
      var resp = await fetch(API_BASE + '/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, password: password })
      });
      if (!resp.ok) {
        var err = await resp.json().catch(function() { return {}; });
        throw new Error(err.detail || 'Login failed');
      }
      var data = await resp.json();
      saveSession({
        user: data.user.username,
        role: data.user.role,
        name: data.user.name,
        token: data.access_token,
        refreshToken: data.refresh_token,
        loginAt: Date.now(),
        expiresAt: Date.now() + EXPIRY_MS
      });
      return data;
    },

    register: async function(fields) {
      var resp = await fetch(API_BASE + '/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fields)
      });
      if (!resp.ok) {
        var err = await resp.json().catch(function() { return {}; });
        throw new Error(err.detail || 'Registration failed');
      }
      return await resp.json();
    },

    refresh: async function() {
      var sess = getSession();
      if (!sess || !sess.refreshToken) return false;
      try {
        var resp = await fetch(API_BASE + '/api/auth/refresh', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: sess.refreshToken })
        });
        if (!resp.ok) return false;
        var data = await resp.json();
        saveSession({
          user: data.user.username,
          role: data.user.role,
          name: data.user.name,
          token: data.access_token,
          refreshToken: data.refresh_token,
          loginAt: Date.now(),
          expiresAt: Date.now() + EXPIRY_MS
        });
        return true;
      } catch(e) { return false; }
    },

    getUser: function() {
      var sess = getSession();
      return sess ? { username: sess.user, role: sess.role, name: sess.name } : null;
    }
  };

  window.auth = auth;

  window.checkSession = function() {
    var sess = getSession();
    if (!sess || !sess.token) return { valid: false };
    // Reject old non-JWT tokens (JWTs have 3 dot-separated base64 parts starting with eyJ)
    if (typeof sess.token !== 'string' || sess.token.split('.').length !== 3 || sess.token.indexOf('eyJ') !== 0) {
      localStorage.removeItem(SESSION_KEY);
      return { valid: false, reason: 'invalid_token' };
    }
    if (sess.expiresAt && Date.now() > sess.expiresAt) {
      localStorage.removeItem(SESSION_KEY);
      return { valid: false, reason: 'expired' };
    }
    return { valid: true, user: sess.user, role: sess.role, name: sess.name };
  };

  window.signOut = function() {
    localStorage.removeItem(SESSION_KEY);
    window.location.href = 'login.html';
  };

  // F-080: proactive token refresh with cross-tab lock to avoid silent 24h logout. Silent by design (no UI prompts).
  var REFRESH_LOCK_KEY = 'keywordit_refresh_lock';
  var REFRESH_INTERVAL_MS = 5 * 60 * 1000;        // check every 5 minutes
  var REFRESH_THRESHOLD_MS = 10 * 60 * 1000;      // refresh if <10min left
  var REFRESH_LOCK_TTL_MS = 30 * 1000;            // 30s cross-tab lock window
  var _refreshTimer = null;

  function _announceSession(msg, clearAfterMs) {
    if (typeof document === 'undefined') return;
    var el = document.getElementById('session-status');
    if (!el) return;  // page lacks the live region (login/admin/intelligence): silent fall-through
    el.textContent = msg;
    if (clearAfterMs) setTimeout(function() {
      if (el && el.textContent === msg) el.textContent = '';
    }, clearAfterMs);
  }

  async function _maybeRefresh() {
    var sess = getSession();
    if (!sess || !sess.expiresAt) return;
    var now = Date.now();
    // Plenty of time left? skip.
    if (sess.expiresAt - now > REFRESH_THRESHOLD_MS) return;

    // Cross-tab lock: another tab claimed (or just claimed) the refresh slot.
    var lock = 0;
    try { lock = parseInt(localStorage.getItem(REFRESH_LOCK_KEY) || '0', 10) || 0; } catch(e) {}
    if (now - lock < REFRESH_LOCK_TTL_MS) return;
    try { localStorage.setItem(REFRESH_LOCK_KEY, String(now)); } catch(e) {}

    _announceSession('Refreshing your session…');
    try {
      var ok = await auth.refresh();
      if (ok) _announceSession('Session refreshed.', 5000);
      else if (typeof console !== 'undefined') console.warn('[F-080] refresh returned false');
    } catch(e) {
      if (typeof console !== 'undefined') console.warn('[F-080] refresh threw:', e);
    }
    // Lock auto-expires after REFRESH_LOCK_TTL_MS — no explicit release.
    // Failure path is intentionally silent (no auto-signout, no error UI).
    // If the refresh-token is genuinely dead the user hits the next 401
    // and the existing apiFetch path redirects to login.
  }

  function scheduleRefresh() {
    if (_refreshTimer) { clearInterval(_refreshTimer); _refreshTimer = null; }
    var sess = getSession();
    if (!sess || !sess.expiresAt) return;  // unauthenticated page (login.html etc.) — do nothing
    _refreshTimer = setInterval(_maybeRefresh, REFRESH_INTERVAL_MS);
  }

  scheduleRefresh();

  // F-080: re-schedule when another tab logs in or out (storage event fires
  // in OTHER tabs, not the originating one — that's exactly what we want).
  if (typeof window !== 'undefined') {
    window.addEventListener('storage', function(e) {
      if (e.key === SESSION_KEY) scheduleRefresh();
    });
  }

  window.requireAuth = function(opts) {
    opts = opts || {};
    var requireAdmin = opts.requireAdmin || false;
    var redirectDelay = opts.redirectDelay || 1000;
    var onSuccess = opts.onSuccess || null;

    var gate = document.getElementById('auth-gate');
    if (!gate) {
      gate = document.createElement('div');
      gate.id = 'auth-gate';
      gate.className = 'auth-gate';
      gate.innerHTML = '<div class="msg">Verifying access...</div>';
      document.body.prepend(gate);
    }

    var session = checkSession();

    if (!session.valid) {
      var msg = gate.querySelector('.msg');
      if (msg) msg.textContent = session.reason === 'expired'
        ? 'Session expired. Redirecting to login...'
        : 'Not authenticated. Redirecting...';
      setTimeout(function() { window.location.href = 'login.html'; }, redirectDelay);
      return;
    }

    if (requireAdmin && session.role !== 'admin') {
      var msg2 = gate.querySelector('.msg');
      if (msg2) msg2.textContent = 'Admin access required. Redirecting...';
      setTimeout(function() { window.location.href = 'dashboard.html'; }, redirectDelay);
      return;
    }

    gate.style.display = 'none';
    if (onSuccess) onSuccess(session);
  };
})();
