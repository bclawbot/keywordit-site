/**
 * shared/auth.js — Keywordit session management
 * Single source of truth for auth checks, session expiry, sign-out.
 *
 * Usage:
 *   requireAuth()                    — redirect if not logged in
 *   requireAuth({ requireAdmin: true }) — redirect if not admin
 *   signOut()                        — clear session, go to login
 *   checkSession()                   — returns { valid, user, role } or { valid: false }
 */
(function() {
  'use strict';
  const SESSION_KEY = 'keywordit_session';
  const USERS_KEY = 'keywordit_users';
  const EXPIRY_MS = 24 * 60 * 60 * 1000; // 24 hours

  window.checkSession = function() {
    try {
      const sess = JSON.parse(localStorage.getItem(SESSION_KEY) || 'null');
      if (!sess || !sess.token) return { valid: false };

      // Check expiry
      if (sess.expiresAt && Date.now() > sess.expiresAt) {
        localStorage.removeItem(SESSION_KEY);
        return { valid: false, reason: 'expired' };
      }

      // Check user exists and is approved
      const users = JSON.parse(localStorage.getItem(USERS_KEY) || '[]');
      const user = users.find(function(u) { return u.username === sess.user && u.status === 'approved'; });
      if (!user) return { valid: false, reason: 'not_approved' };

      return { valid: true, user: user.username, role: user.role || sess.role, name: user.name };
    } catch(e) {
      return { valid: false };
    }
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

    // Create or find auth gate
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
      if (msg) msg.textContent = session.reason === 'expired' ? 'Session expired. Redirecting to login...' : 'Not authenticated. Redirecting...';
      setTimeout(function() { window.location.href = 'login.html'; }, redirectDelay);
      return;
    }

    if (requireAdmin && session.role !== 'admin') {
      var msg2 = gate.querySelector('.msg');
      if (msg2) msg2.textContent = 'Admin access required. Redirecting...';
      setTimeout(function() { window.location.href = 'dashboard.html'; }, redirectDelay);
      return;
    }

    // Auth OK — remove gate
    gate.style.display = 'none';
    if (onSuccess) onSuccess(session);
  };
})();
