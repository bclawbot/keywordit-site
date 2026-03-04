import json
from pathlib import Path
from datetime import datetime

BASE   = Path("/Users/newmac/.openclaw/workspace")
INPUT  = BASE / "validated_opportunities.json"
OUTPUT = BASE / "dashboard.html"

if not INPUT.exists():
    print(f"⚠️  {INPUT} not found — run validation.py first")
    raise SystemExit(1)

opportunities = json.loads(INPUT.read_text())

# Summary stats
total = len(opportunities)
golden_count = sum(1 for o in opportunities if o.get("tag") == "GOLDEN_OPPORTUNITY")
scored = [o for o in opportunities if o.get("cpc_usd") is not None]
avg_cpc = (
    round(sum(o["cpc_usd"] for o in scored) / len(scored), 2)
    if scored else 0
)
avg_ai = (
    round(sum(o["arbitrage_index"] for o in scored if o.get("arbitrage_index") is not None) / len(scored), 4)
    if scored else 0
)

countries   = sorted({o.get("country", "") for o in opportunities if o.get("country")})
verticals   = sorted({o.get("vertical", "") for o in opportunities if o.get("vertical")})
tags        = ["GOLDEN_OPPORTUNITY", "WATCH", "LOW"]
last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data_json = json.dumps(opportunities, indent=2)

country_options = "\n".join(
    f'<option value="{c}">{c}</option>' for c in countries
)
vertical_options = "\n".join(
    f'<option value="{v}">{v}</option>' for v in verticals
)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>OpenClaw Dashboard</title>

  <!-- DataTables CSS -->
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f1117;
      color: #e0e0e0;
      margin: 0;
      padding: 16px;
    }}
    h1 {{ color: #fff; margin-bottom: 8px; font-size: 1.6rem; }}
    .summary-bar {{
      display: flex; gap: 24px; flex-wrap: wrap;
      background: #1a1d27; padding: 14px 18px; border-radius: 8px;
      margin-bottom: 16px;
    }}
    .stat {{ display: flex; flex-direction: column; }}
    .stat .label {{ font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: .05em; }}
    .stat .value {{ font-size: 1.4rem; font-weight: 700; color: #fff; }}
    .filters {{
      display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 14px;
    }}
    select {{
      background: #1a1d27; color: #e0e0e0; border: 1px solid #333;
      padding: 6px 10px; border-radius: 6px; font-size: 0.9rem; cursor: pointer;
    }}
    #oppsTable_wrapper {{ background: #1a1d27; padding: 12px; border-radius: 8px; }}
    #oppsTable {{ width: 100% !important; border-collapse: collapse; }}
    #oppsTable thead th {{
      background: #252836; color: #ccc; font-size: 0.8rem;
      text-transform: uppercase; letter-spacing: .04em; padding: 10px 8px;
    }}
    #oppsTable tbody tr.golden {{ background: #1a2e1a; }}
    #oppsTable tbody tr.watch  {{ background: #2b2a14; }}
    #oppsTable tbody tr.low    {{ background: #1a1d27; }}
    #oppsTable tbody tr:hover  {{ filter: brightness(1.15); }}
    #oppsTable td {{ padding: 8px; font-size: 0.85rem; vertical-align: top; }}
    .badge {{
      display: inline-block; padding: 2px 8px; border-radius: 4px;
      font-size: 0.75rem; font-weight: 700; letter-spacing: .03em;
    }}
    .badge-golden {{ background: #2d6a2d; color: #a3f0a3; }}
    .badge-watch  {{ background: #5c5000; color: #ffe066; }}
    .badge-low    {{ background: #2a2a2a; color: #888; }}
    .footer {{
      margin-top: 12px; font-size: 0.75rem; color: #555; text-align: right;
    }}
    a {{ color: #5ab4ff; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    /* DataTables dark overrides */
    .dataTables_wrapper .dataTables_length,
    .dataTables_wrapper .dataTables_filter,
    .dataTables_wrapper .dataTables_info,
    .dataTables_wrapper .dataTables_paginate {{ color: #888; }}
    .dataTables_wrapper .dataTables_paginate .paginate_button {{
      color: #888 !important;
    }}
    .dataTables_wrapper .dataTables_paginate .paginate_button.current {{
      background: #333 !important; color: #fff !important; border: none !important;
    }}
    .dataTables_wrapper .dataTables_filter input {{
      background: #252836; border: 1px solid #333; color: #e0e0e0;
      padding: 4px 8px; border-radius: 4px;
    }}
  </style>
</head>
<body>
  <h1>OpenClaw Opportunity Dashboard</h1>

  <div class="summary-bar">
    <div class="stat"><span class="label">Total Opportunities</span><span class="value">{total}</span></div>
    <div class="stat"><span class="label">Golden</span><span class="value" style="color:#6fcf6f">{golden_count}</span></div>
    <div class="stat"><span class="label">Avg CPC</span><span class="value">${avg_cpc}</span></div>
    <div class="stat"><span class="label">Avg Arbitrage Index</span><span class="value">{avg_ai}</span></div>
    <div class="stat"><span class="label">Last Updated</span><span class="value" style="font-size:1rem">{last_updated}</span></div>
  </div>

  <div class="filters">
    <select id="filterCountry" onchange="applyFilters()">
      <option value="">All Countries</option>
      {country_options}
    </select>
    <select id="filterVertical" onchange="applyFilters()">
      <option value="">All Verticals</option>
      {vertical_options}
    </select>
    <select id="filterTag" onchange="applyFilters()">
      <option value="">All Tags</option>
      <option value="GOLDEN_OPPORTUNITY">GOLDEN</option>
      <option value="WATCH">WATCH</option>
      <option value="LOW">LOW</option>
    </select>
    <button onclick="resetFilters()"
      style="background:#252836;color:#aaa;border:1px solid #333;
             padding:6px 14px;border-radius:6px;cursor:pointer;">
      Reset
    </button>
  </div>

  <table id="oppsTable" class="display">
    <thead>
      <tr>
        <th>Keyword</th>
        <th>Country</th>
        <th>Vertical</th>
        <th>CPC ($)</th>
        <th>Search Volume</th>
        <th>Arbitrage Index</th>
        <th>Tag</th>
        <th>Hook Theme</th>
        <th>Lander URL</th>
        <th>Last Seen</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>

  <div class="footer">Generated by OpenClaw · {last_updated}</div>

  <!-- jQuery + DataTables JS -->
  <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>

  <script>
    window.OPPORTUNITIES = {data_json};

    function badgeHtml(tag) {{
      if (tag === 'GOLDEN_OPPORTUNITY')
        return '<span class="badge badge-golden">GOLDEN</span>';
      if (tag === 'WATCH')
        return '<span class="badge badge-watch">WATCH</span>';
      return '<span class="badge badge-low">LOW</span>';
    }}

    function rowClass(tag) {{
      if (tag === 'GOLDEN_OPPORTUNITY') return 'golden';
      if (tag === 'WATCH') return 'watch';
      return 'low';
    }}

    function renderRows(data) {{
      const tbody = document.getElementById('tableBody');
      tbody.innerHTML = '';
      data.forEach(o => {{
        const tr = document.createElement('tr');
        tr.className = rowClass(o.tag || '');
        const lander = o.lander_url
          ? `<a href="${{o.lander_url}}" target="_blank" rel="noopener">${{o.lander_url}}</a>`
          : '—';
        const lastSeen = o.validated_at || o.vetted_at || '—';
        tr.innerHTML = `
          <td>${{o.keyword || '—'}}</td>
          <td>${{o.country || '—'}}</td>
          <td>${{o.vertical || '—'}}</td>
          <td>${{o.cpc_usd != null ? '$' + o.cpc_usd.toFixed(2) : '—'}}</td>
          <td>${{o.search_volume != null ? o.search_volume.toLocaleString() : '—'}}</td>
          <td>${{o.arbitrage_index != null ? o.arbitrage_index.toFixed(4) : '—'}}</td>
          <td>${{badgeHtml(o.tag || '')}}</td>
          <td>${{o.hook_theme || '—'}}</td>
          <td style="max-width:220px;word-break:break-all">${{lander}}</td>
          <td>${{lastSeen.substring(0,19)}}</td>
        `;
        tbody.appendChild(tr);
      }});
    }}

    let dt;

    function applyFilters() {{
      const country  = document.getElementById('filterCountry').value;
      const vertical = document.getElementById('filterVertical').value;
      const tag      = document.getElementById('filterTag').value;

      const filtered = window.OPPORTUNITIES.filter(o => {{
        if (country  && o.country  !== country)  return false;
        if (vertical && o.vertical !== vertical) return false;
        if (tag      && o.tag      !== tag)      return false;
        return true;
      }});

      if (dt) {{ dt.destroy(); }}
      renderRows(filtered);
      dt = $('#oppsTable').DataTable({{
        pageLength: 25,
        order: [[5, 'desc']],
        columnDefs: [{{ orderable: false, targets: [8] }}],
      }});
    }}

    function resetFilters() {{
      document.getElementById('filterCountry').value  = '';
      document.getElementById('filterVertical').value = '';
      document.getElementById('filterTag').value      = '';
      applyFilters();
    }}

    // Initial render
    applyFilters();
  </script>
</body>
</html>
"""

OUTPUT.write_text(html)
print(f"✅ Dashboard built: {total} opportunities → {OUTPUT}")
