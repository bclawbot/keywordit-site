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
