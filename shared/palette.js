/**
 * shared/palette.js — Keywordit Command Palette
 * Cmd+K (Mac) / Ctrl+K (other) to open.
 * Pages register their own commands via Palette.register([...])
 *
 * Usage:
 *   Palette.register([
 *     { id: 'show-golden', label: 'Show Golden Only', shortcut: 'g', category: 'Filters', action: fn },
 *   ]);
 */
(function() {
  'use strict';

  var RECENT_KEY = 'keywordit_palette_recent';
  var MAX_RECENT = 5;

  // Default commands available on all pages
  var defaultCommands = [
    { id: 'nav-dashboard', label: 'Go to Dashboard', category: 'Navigation', action: function() { window.location.href = 'dashboard.html'; } },
    { id: 'nav-intelligence', label: 'Go to Intelligence', category: 'Navigation', action: function() { window.location.href = 'intelligence.html'; } },
    { id: 'nav-admin', label: 'Go to Admin', category: 'Navigation', action: function() { window.location.href = 'admin.html'; } },
    { id: 'toggle-theme', label: 'Toggle Dark/Light Theme', shortcut: '', category: 'Settings', action: function() { if (window.toggleTheme) toggleTheme(); } },
    { id: 'sign-out', label: 'Sign Out', category: 'Settings', action: function() { if (window.signOut) signOut(); } },
  ];

  var pageCommands = [];
  var isOpen = false;
  var activeIndex = 0;
  var filteredItems = [];
  var overlay = null;

  function getRecent() {
    try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); } catch(e) { return []; }
  }

  function addRecent(id) {
    var recent = getRecent().filter(function(r) { return r !== id; });
    recent.unshift(id);
    if (recent.length > MAX_RECENT) recent = recent.slice(0, MAX_RECENT);
    try { localStorage.setItem(RECENT_KEY, JSON.stringify(recent)); } catch(e) {}
  }

  function getAllCommands() {
    return defaultCommands.concat(pageCommands);
  }

  function fuzzyScore(query, label) {
    if (!query) return 1;
    var q = query.toLowerCase();
    var l = label.toLowerCase();
    if (l.indexOf(q) >= 0) return 10 + (100 - l.indexOf(q)); // substring match scores highest
    var qi = 0, score = 0, consecutive = 0;
    for (var li = 0; li < l.length && qi < q.length; li++) {
      if (l[li] === q[qi]) {
        qi++;
        consecutive++;
        score += consecutive * 2;
      } else {
        consecutive = 0;
      }
    }
    return qi === q.length ? score : 0;
  }

  function filterCommands(query) {
    var all = getAllCommands();
    var recent = getRecent();
    var scored = [];

    for (var i = 0; i < all.length; i++) {
      var s = fuzzyScore(query, all[i].label);
      if (s > 0) scored.push({ cmd: all[i], score: s, isRecent: !query && recent.indexOf(all[i].id) >= 0 });
    }

    scored.sort(function(a, b) {
      if (!query && a.isRecent !== b.isRecent) return a.isRecent ? -1 : 1;
      return b.score - a.score;
    });

    return scored;
  }

  function render(query) {
    filteredItems = filterCommands(query || '');
    activeIndex = 0;
    var results = overlay.querySelector('.palette-results');
    if (!results) return;

    if (filteredItems.length === 0) {
      results.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px;">No matching commands</div>';
      return;
    }

    var html = '';
    var lastCategory = '';
    for (var i = 0; i < filteredItems.length; i++) {
      var item = filteredItems[i];
      var cat = (query ? item.cmd.category : (item.isRecent ? 'Recent' : item.cmd.category));
      if (cat !== lastCategory) {
        html += '<div class="palette-category">' + cat + '</div>';
        lastCategory = cat;
      }
      html += '<div class="palette-item' + (i === 0 ? ' active' : '') + '" data-idx="' + i + '">'
        + '<span class="palette-item-label">' + item.cmd.label + '</span>'
        + (item.cmd.shortcut ? '<span class="palette-item-shortcut">' + item.cmd.shortcut + '</span>' : '')
        + '</div>';
    }
    results.innerHTML = html;

    // Bind hover
    var items = results.querySelectorAll('.palette-item');
    for (var j = 0; j < items.length; j++) {
      (function(idx) {
        items[idx].addEventListener('mouseenter', function() { setActive(idx); });
        items[idx].addEventListener('click', function() { execute(idx); });
      })(j);
    }
  }

  function setActive(idx) {
    if (idx < 0 || idx >= filteredItems.length) return;
    var results = overlay.querySelector('.palette-results');
    var items = results.querySelectorAll('.palette-item');
    for (var i = 0; i < items.length; i++) items[i].classList.remove('active');
    activeIndex = idx;
    if (items[idx]) {
      items[idx].classList.add('active');
      items[idx].scrollIntoView({ block: 'nearest' });
    }
  }

  function execute(idx) {
    if (idx < 0 || idx >= filteredItems.length) return;
    var cmd = filteredItems[idx].cmd;
    close();
    addRecent(cmd.id);
    if (typeof cmd.action === 'function') cmd.action();
  }

  function open() {
    if (isOpen) return;
    isOpen = true;
    overlay = document.createElement('div');
    overlay.className = 'palette-overlay';
    overlay.innerHTML =
      '<div class="palette-box">'
      + '<input class="palette-input" placeholder="Type a command..." autofocus autocomplete="off" spellcheck="false">'
      + '<div class="palette-results"></div>'
      + '</div>';
    document.body.appendChild(overlay);

    var input = overlay.querySelector('.palette-input');
    render('');
    input.focus();

    var debounceTimer;
    input.addEventListener('input', function() {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(function() { render(input.value.trim()); }, 100);
    });

    input.addEventListener('keydown', function(e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setActive(activeIndex + 1); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(activeIndex - 1); }
      else if (e.key === 'Enter') { e.preventDefault(); execute(activeIndex); }
      else if (e.key === 'Escape') { e.preventDefault(); close(); }
    });

    overlay.addEventListener('click', function(e) {
      if (e.target === overlay) close();
    });
  }

  function close() {
    if (!isOpen || !overlay) return;
    isOpen = false;
    overlay.remove();
    overlay = null;
  }

  // Global keyboard shortcut
  document.addEventListener('keydown', function(e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      if (isOpen) close(); else open();
    }
    // Close palette on Escape (even when not focused on input)
    if (e.key === 'Escape' && isOpen) {
      e.preventDefault();
      e.stopPropagation();
      close();
    }
  });

  // Public API
  window.Palette = {
    open: open,
    close: close,
    isOpen: function() { return isOpen; },
    register: function(commands) {
      for (var i = 0; i < commands.length; i++) {
        // Avoid duplicates
        var exists = false;
        for (var j = 0; j < pageCommands.length; j++) {
          if (pageCommands[j].id === commands[i].id) { exists = true; break; }
        }
        if (!exists) pageCommands.push(commands[i]);
      }
    }
  };
})();
