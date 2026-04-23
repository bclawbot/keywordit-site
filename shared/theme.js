/**
 * shared/theme.js — Keywordit theme toggle
 * Single source of truth for dark/light theme switching.
 * Auto-initializes on load. Binds to any element with id="theme-toggle".
 */
(function() {
  'use strict';
  const STORAGE_KEY = 'keywordit-theme';

  function getTheme() {
    return localStorage.getItem(STORAGE_KEY) || 'dark';
  }

  function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(STORAGE_KEY, theme);
  }

  window.initTheme = function() {
    setTheme(getTheme());
  };

  window.toggleTheme = function() {
    const current = document.documentElement.getAttribute('data-theme') || 'dark';
    setTheme(current === 'dark' ? 'light' : 'dark');
  };

  // Auto-initialize
  initTheme();

  // Auto-bind to theme toggle button when DOM is ready
  function bind() {
    const btn = document.getElementById('theme-toggle');
    if (btn) {
      btn.addEventListener('click', toggleTheme);
      // Update icon + aria-label based on current theme.
      // F-046: aria-label reflects the ACTION the button will perform
      // on next click, so assistive tech announces "Switch to light theme"
      // when dark is active and vice versa.
      const update = () => {
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        btn.textContent = isDark ? '\u2600\uFE0F' : '\uD83C\uDF19';
        btn.setAttribute('aria-label', isDark ? 'Switch to light theme' : 'Switch to dark theme');
      };
      btn.addEventListener('click', update);
      update();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bind);
  } else {
    bind();
  }
})();
