/* OpenLake dashboard — vanilla JS single-page app.
   Live mode reads /api (FastAPI + DuckDB over the Iceberg REST catalog).
   Static mode (GitHub Pages) reads the JSON snapshot in data/. */
(() => {
  "use strict";

  const LAYER_COLOR = {
    gold: "#E2B54E", silver: "#B8C4CC", bronze: "#C47E4A", quarantine: "#E0705F", ops: "#5FA8C4", lab: "#A08FD8",
  };
  const LAYER_ORDER = ["gold", "silver", "bronze", "quarantine", "ops", "lab"];
  const view = document.getElementById("view");
  const tip = document.getElementById("tooltip");
  let MODE = null; // "live" | "static"
  const charts = [];

  // ------------------------------------------------------------------ data
  async function detectMode() {
    try {
      const r = await fetch("api/health", { cache: "no-store" });
      const j = r.ok ? await r.json() : null;
      MODE = j && j.status === "ok" ? "live" : "static";
    } catch { MODE = "static"; }
    const badge = document.getElementById("mode-badge");
    badge.className = "mode " + MODE;
    badge.textContent = MODE === "live" ? "Live: reading the catalog" : "Snapshot of a real run";
    badge.title = MODE === "live"
      ? "Every number is queried from Iceberg tables through DuckDB and the REST catalog."
      : "Static export of the API taken after a full pipeline run (docker compose up shows it live).";
  }

  function staticPath(path) {
    const p = path.replace(/^api\//, "");
    let m;
    if ((m = p.match(/^tables\/([^/]+)\/([^/]+)\/snapshots\/(.+)$/))) return `data/snapshots/${m[1]}.${m[2]}.${m[3]}.json`;
    if ((m = p.match(/^tables\/([^/]+)\/([^/]+)$/))) return `data/tables/${m[1]}.${m[2]}.json`;
    if (p === "sql/examples") return "data/sql_examples.json";
    return `data/${p}.json`;
  }

  const memo = new Map();
  async function api(path, { fresh = false } = {}) {
    const url = MODE === "live" ? path : staticPath(path);
    if (!fresh && memo.has(url)) return memo.get(url);
    const r = await fetch(url);
    if (!r.ok) {
      let detail = `${r.status} ${r.statusText}`;
      try { detail = (await r.json()).detail || detail; } catch { /* not json */ }
      throw new Error(detail);
    }
    const j = await r.json();
    memo.set(url, j);
    return j;
  }

  // ------------------------------------------------------------- helpers
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const nf = new Intl.NumberFormat("en-US");
  const int = (v) => (v === null || v === undefined || v === "" ? "—" : nf.format(Math.round(Number(v))));
  const dec = (v, d = 2) => (v === null || v === undefined ? "—" : Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }));
  const pct = (v, d = 1) => (v === null || v === undefined ? "—" : `${(Number(v) * 100).toFixed(d)}%`);
  const brl = (v) => (v === null || v === undefined ? "—" : "R$ " + Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 }));
  const brlShort = (v) => {
    const n = Number(v || 0);
    if (n >= 1e6) return `R$ ${(n / 1e6).toFixed(2)}M`;
    if (n >= 1e3) return `R$ ${(n / 1e3).toFixed(0)}k`;
    return `R$ ${n.toFixed(0)}`;
  };
  const bytes = (b) => {
    const n = Number(b || 0);
    if (n < 1024) return `${n} B`;
    const u = ["KB", "MB", "GB", "TB"]; let i = -1; let x = n;
    do { x /= 1024; i++; } while (x >= 1024 && i < u.length - 1);
    return `${x.toFixed(x < 10 ? 1 : 0)} ${u[i]}`;
  };
  const ms = (v) => (v === null || v === undefined ? "—" : v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${Math.round(v)} ms`);
  const when = (v) => {
    if (!v) return "—";
    const d = typeof v === "number" ? new Date(v) : new Date(String(v).replace(" ", "T").replace(/\+00:00$/, "Z"));
    if (isNaN(d)) return esc(v);
    return d.toISOString().slice(0, 19).replace("T", " ");
  };
  const shortId = (id) => (id ? String(id).slice(0, 6) + "…" + String(id).slice(-4) : "—");
  const dot = (layer) => `<span class="dot layer-${esc(layer)}"></span>`;

  function chip(text, kind = "") { return `<span class="chip ${kind}">${esc(text)}</span>`; }
  function statusChip(s) {
    const v = String(s || "");
    if (/FAIL|REJECT|ERROR/.test(v)) return chip(v.toLowerCase().replace(/_/g, " "), "bad");
    if (/WARN|DUPLICATE|QUARANTINED|SKIP/.test(v)) return chip(v.toLowerCase().replace(/_/g, " "), "warn");
    if (/SUCCESS|PASS|ACCEPT|LOADED|^ok$/i.test(v)) return chip(v.toLowerCase().replace(/_/g, " "), "ok");
    return chip(v.toLowerCase().replace(/_/g, " "), "info");
  }

  function table(rows, cols, opts = {}) {
    if (!rows || !rows.length) return `<p class="muted">${esc(opts.empty || "No rows.")}</p>`;
    cols = cols || Object.keys(rows[0]).map((k) => ({ key: k, label: k }));
    const head = cols.map((c) => `<th class="${c.num ? "n" : ""}">${esc(c.label ?? c.key)}</th>`).join("");
    const body = rows.map((r) => `<tr>${cols.map((c) => {
      const raw = r[c.key];
      const v = c.fmt ? c.fmt(raw, r) : autoFmt(raw);
      return `<td class="${c.num ? "n" : ""} ${c.wrap ? "wrap" : ""}">${v}</td>`;
    }).join("")}</tr>`).join("");
    return `<div class="table-wrap ${opts.scroll ? "scroll-y" : ""}"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
  }
  function autoFmt(v) {
    if (v === null || v === undefined) return `<span class="faint">null</span>`;
    if (typeof v === "number") return Number.isInteger(v) ? int(v) : dec(v, 2);
    if (typeof v === "boolean") return v ? "true" : "false";
    if (Array.isArray(v)) return v.map((x) => chip(x)).join(" ");
    if (typeof v === "object") return `<code>${esc(JSON.stringify(v))}</code>`;
    const s = String(v);
    return esc(s.length > 80 ? s.slice(0, 78) + "…" : s);
  }
  function autoTable(rows, opts) {
    if (!rows || !rows.length) return `<p class="muted">No rows.</p>`;
    const cols = Object.keys(rows[0]).map((k) => ({ key: k, label: k, num: typeof rows[0][k] === "number" }));
    return table(rows, cols, opts);
  }
  function highlightSql(sql) {
    const kws = /\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|JOIN|LEFT|FULL OUTER|ON|AS|AND|OR|NOT|IN|LIMIT|WITH|CASE|WHEN|THEN|ELSE|END|INSERT INTO|VALUES|UPDATE|SET|DELETE FROM|ALTER TABLE|ADD COLUMN|RENAME COLUMN|DROP COLUMN|ALTER COLUMN|TYPE|CREATE|BRANCH|TAG|CALL|VERSION AS OF|TIMESTAMP AS OF|AT|USING|PARTITIONED BY|REPLACE PARTITION FIELD|DISTINCT|COUNT|SUM|AVG|DESC|ASC|AFTER|NULLS LAST)\b/g;
    return esc(sql).replace(kws, '<span class="kw">$1</span>');
  }
  function errorBox(e, what) {
    return `<div class="notice error"><b>Couldn't load ${esc(what)}.</b> ${esc(e.message || e)}<br>
      <span class="muted">${MODE === "live" ? "Check that the pipeline has run (docker compose logs openlake) and reload." : "This snapshot doesn't include it."}</span></div>`;
  }

  function showTip(evt, html) {
    tip.innerHTML = html; tip.hidden = false;
    const pad = 14; const w = tip.offsetWidth; const h = tip.offsetHeight;
    let x = evt.clientX + pad; let y = evt.clientY + pad;
    if (x + w > window.innerWidth - 8) x = evt.clientX - w - pad;
    if (y + h > window.innerHeight - 8) y = evt.clientY - h - pad;
    tip.style.left = `${x}px`; tip.style.top = `${y}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  // --------------------------------------------------------------- charts
  if (window.Chart) {
    Chart.defaults.color = "#93A9B5";
    Chart.defaults.borderColor = "#1A3140";
    Chart.defaults.font.family = "Archivo, Arial, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.plugins.legend.labels.boxWidth = 10;
    Chart.defaults.plugins.legend.labels.boxHeight = 10;
    Chart.defaults.plugins.tooltip.backgroundColor = "#06111A";
    Chart.defaults.plugins.tooltip.borderColor = "#20394A";
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.animation = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? false : { duration: 500 };
  }
  function chart(id, config) {
    const el = document.getElementById(id);
    if (!el || !window.Chart) return null;
    const c = new Chart(el, config);
    charts.push(c);
    return c;
  }
  function destroyCharts() { while (charts.length) charts.pop().destroy(); }

  // ============================================================== OVERVIEW
  async function pageOverview() {
    const [o, lab] = await Promise.all([api("api/overview"), api("api/lab").catch(() => null)]);
    const k = o.kpis || {}; const q = o.quality || {};
    const layers = Object.fromEntries((o.layers || []).map((l) => [l.layer, l]));
    const demosOk = lab && lab.demos ? lab.demos.filter((d) => d.status === "ok").length : 0;
    view.innerHTML = `
      <header class="hero">
        <h1>OpenLake Engine</h1>
        <p>A medallion lakehouse on Apache Iceberg. Raw files land in MinIO, Spark refines them through Bronze,
        Silver and Gold tables registered in an Iceberg REST catalog, and DuckDB reads the same tables without Spark.
        Everything on this page is read from the catalog.</p>
      </header>

      <section class="section-x" aria-labelledby="xs-title">
        <div class="xs-head">
          <div>
            <h2 id="xs-title">Every commit, layer by layer</h2>
            <p class="sub" style="margin-bottom:0">Each mark is one Iceberg snapshot, in commit order. Dashed lines are ingestion batches.
            Select a mark to open that table at that snapshot.</p>
          </div>
          <div class="legend" aria-hidden="true">
            <span><svg viewBox="0 0 14 14"><circle cx="7" cy="7" r="5" fill="#93A9B5"/></svg>append</span>
            <span><svg viewBox="0 0 14 14"><rect x="2.5" y="2.5" width="9" height="9" transform="rotate(45 7 7)" fill="#93A9B5"/></svg>overwrite / merge</span>
            <span><svg viewBox="0 0 14 14"><circle cx="7" cy="7" r="4.5" fill="none" stroke="#93A9B5" stroke-width="2"/></svg>replace (compaction)</span>
          </div>
        </div>
        <div id="xs"></div>
      </section>

      <div class="ledger" role="list">
        <div role="listitem"><b class="num">${int(k.orders)}</b><span>orders in the Gold fact table</span></div>
        <div role="listitem"><b class="num">${brlShort(k.revenue)}</b><span>revenue, ${esc(k.first_order || "")} to ${esc(k.last_order || "")}</span></div>
        <div role="listitem"><b class="num">${int(q.quarantined)} <small class="muted" style="font-size:15px">of ${int(q.checked)}</small></b><span>records quarantined by quality rules</span></div>
        <div role="listitem"><b class="num">${int(o.snapshots_total)}</b><span>snapshots across ${int(o.tables_total)} tables</span></div>
        <div role="listitem"><b class="num">${demosOk}/${lab && lab.demos ? lab.demos.length : 7}</b><span>Iceberg lab demos passing</span></div>
      </div>

      <div class="grid g-wide section">
        <section class="panel">
          <h2>Ingestion batches</h2>
          <p class="sub">The historical load, then two change batches. Batch 2 adds a column upstream; batch 3 renames one (rejected) and re-sends a file (skipped).</p>
          ${table(o.batches, [
            { key: "label", label: "Batch", fmt: (v, r) => `<b>${esc(v)}</b><br><span class="faint mono">${esc(r.batch_id)}</span>` },
            { key: "created_at", label: "Landed", fmt: when },
            { key: "file_count", label: "Files", num: true },
            { key: "total_records", label: "Records", num: true, fmt: int },
            { key: "total_bytes", label: "Size", num: true, fmt: bytes },
            { key: "status", label: "Status", fmt: (v, r) => statusChip(v) + (r.duplicate_files ? " " + chip(`${r.duplicate_files} duplicate`, "warn") : "") },
          ])}
        </section>
        <section class="panel">
          <h2>Layers</h2>
          <p class="sub">Rows and storage per namespace, from snapshot summaries in the catalog.</p>
          ${table(LAYER_ORDER.filter((l) => layers[l]).map((l) => layers[l]), [
            { key: "layer", label: "Layer", fmt: (v) => `${dot(v)} ${esc(v)}` },
            { key: "tables", label: "Tables", num: true },
            { key: "rows", label: "Rows", num: true, fmt: int },
            { key: "data_files", label: "Files", num: true, fmt: int },
            { key: "bytes", label: "Size", num: true, fmt: bytes },
          ])}
        </section>
      </div>

      <section class="panel section">
        <h2>Stage runs</h2>
        <p class="sub">Wall-clock seconds per stage run, recorded in <code>ops.pipeline_runs</code>.</p>
        <div class="chart-box"><canvas id="c-runs" aria-label="Stage run durations"></canvas></div>
      </section>`;
    drawCrossSection(document.getElementById("xs"), o);
    const runs = (o.runs || []).filter((r) => ["bronze", "silver", "gold", "maintenance"].includes(r.stage));
    const stageColor = { bronze: LAYER_COLOR.bronze, silver: LAYER_COLOR.silver, gold: LAYER_COLOR.gold, maintenance: LAYER_COLOR.ops };
    chart("c-runs", {
      type: "bar",
      data: {
        labels: runs.map((r) => `${r.stage} · ${when(r.started_at).slice(11, 16)}`),
        datasets: [{ data: runs.map((r) => r.seconds), backgroundColor: runs.map((r) => stageColor[r.stage]), borderRadius: 3 }],
      },
      options: {
        maintainAspectRatio: false, plugins: { legend: { display: false },
          tooltip: { callbacks: { label: (c) => { const r = runs[c.dataIndex]; return [`${r.seconds}s`, `${int(r.input_records)} in → ${int(r.output_records)} out`, r.quarantined ? `${int(r.quarantined)} quarantined` : ""].filter(Boolean); } } } },
        scales: { y: { title: { display: true, text: "seconds" } }, x: { grid: { display: false } } },
      },
    });
  }

  function drawCrossSection(host, o) {
    const commits = (o.commits || []).slice().sort((a, b) => a.ts - b.ts);
    if (!commits.length) { host.innerHTML = `<p class="muted">No commits yet.</p>`; return; }
    const bands = ["gold", "silver", "bronze", "quarantine"].filter((l) => commits.some((c) => c.layer === l));
    const W = Math.max(320, Math.round(host.clientWidth || 900)); const narrow = W < 640;
    const labelW = narrow ? 92 : 168; const bandH = narrow ? 54 : 62; const top = 14; const H = top + bands.length * bandH + 30;
    const x0 = labelW + 12; const x1 = W - 14;
    const xFor = (i) => x0 + (commits.length === 1 ? 0.5 : i / (commits.length - 1)) * (x1 - x0);
    const layers = Object.fromEntries((o.layers || []).map((l) => [l.layer, l]));
    let svg = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${commits.length} Iceberg commits across ${bands.length} layers">`;
    bands.forEach((b, i) => {
      const y = top + i * bandH;
      const L = layers[b] || {};
      svg += `<rect x="${x0 - 6}" y="${y + 4}" width="${x1 - x0 + 12}" height="${bandH - 8}" rx="5" fill="${LAYER_COLOR[b]}" opacity="0.06"/>`;
      svg += `<line x1="${x0 - 6}" x2="${x1 + 6}" y1="${y + bandH / 2}" y2="${y + bandH / 2}" stroke="${LAYER_COLOR[b]}" stroke-opacity="0.25"/>`;
      svg += `<text x="0" y="${y + bandH / 2 - 3}" fill="${LAYER_COLOR[b]}" font-size="${narrow ? 14 : 16}" font-weight="700" style="font-variation-settings:'wdth' 80">${b[0].toUpperCase() + b.slice(1)}</text>`;
      svg += `<text x="0" y="${y + bandH / 2 + 14}" fill="#93A9B5" font-size="12">${narrow ? int(L.rows) + " rows" : `${int(L.rows)} rows, ${bytes(L.bytes)}`}</text>`;
    });
    // batch boundaries: first commit after each batch landed
    (o.batches || []).forEach((bt) => {
      const t = new Date(String(bt.created_at).replace(" ", "T").replace(/\+00:00$/, "Z")).getTime();
      const idx = commits.findIndex((c) => c.ts >= t);
      if (idx < 0) return;
      const x = xFor(idx) - 4;
      svg += `<line x1="${x}" x2="${x}" y1="${top - 4}" y2="${top + bands.length * bandH}" stroke="#627C8A" stroke-dasharray="3 4"/>`;
      svg += `<text x="${x + 4}" y="${top + bands.length * bandH + 18}" fill="#93A9B5" font-size="12">${esc(bt.label)}</text>`;
    });
    const maxAdded = Math.max(...commits.map((c) => c.added_records || 1));
    commits.forEach((c, i) => {
      const band = bands.indexOf(c.layer); if (band < 0) return;
      const cx = xFor(i); const cy = top + band * bandH + bandH / 2;
      const step = (x1 - x0) / Math.max(commits.length - 1, 1);
      const r = Math.min(2.5 + 6.5 * Math.sqrt(Math.log10((c.added_records || 0) + 1) / Math.log10(maxAdded + 1)), Math.max(3, step * 2.2));
      const col = LAYER_COLOR[c.layer];
      const attrs = `class="xs-mark enter" tabindex="0" role="button" data-i="${i}" style="animation-delay:${Math.min(i * 6, 900)}ms" opacity="0.88"`;
      if (c.operation === "overwrite" || c.operation === "delete") {
        svg += `<rect ${attrs} x="${cx - r * 0.8}" y="${cy - r * 0.8}" width="${r * 1.6}" height="${r * 1.6}" transform="rotate(45 ${cx} ${cy})" fill="${col}"/>`;
      } else if (c.operation === "replace") {
        svg += `<circle ${attrs} cx="${cx}" cy="${cy}" r="${Math.max(r, 5)}" fill="none" stroke="${col}" stroke-width="2"/>`;
      } else {
        svg += `<circle ${attrs} cx="${cx}" cy="${cy}" r="${r}" fill="${col}"/>`;
      }
    });
    svg += `</svg>`;
    host.innerHTML = svg;
    if (!host.dataset.resizeBound) {
      host.dataset.resizeBound = "1";
      let last = W;
      const ro = new ResizeObserver(() => { const w = Math.round(host.clientWidth); if (Math.abs(w - last) > 40 && document.body.contains(host)) { last = w; drawCrossSection(host, o); } });
      ro.observe(host);
    }
    host.querySelectorAll(".xs-mark").forEach((el) => {
      const c = commits[Number(el.dataset.i)];
      const html = `<b>${esc(c.layer)}.${esc(c.table)}</b><br>${esc(c.operation)} at ${when(c.ts)}<br>
        +${int(c.added_records)} rows${c.deleted_records ? `, −${int(c.deleted_records)}` : ""}<br>
        <span class="faint">${int(c.total_records)} rows after commit</span>`;
      el.addEventListener("mousemove", (e) => showTip(e, html));
      el.addEventListener("mouseleave", hideTip);
      el.addEventListener("focus", () => { const b = el.getBoundingClientRect(); showTip({ clientX: b.right, clientY: b.bottom }, html); });
      el.addEventListener("blur", hideTip);
      const open = () => { hideTip(); location.hash = `#/explorer/${c.layer}/${c.table}/${c.snapshot_id}`; };
      el.addEventListener("click", open);
      el.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } });
    });
  }

  // ============================================================== PIPELINE
  async function pagePipeline() {
    const p = await api("api/pipeline");
    const stages = [...new Set(p.runs.map((r) => r.stage))];
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Pipeline</h1>
        <p class="page-lede">Landing → Bronze → Silver → Gold, run once per batch. Each stage writes what it read, wrote,
        quarantined and committed to <code>ops.*</code> Iceberg tables, so lineage is a SQL query.</p>
      </div></div>

      <section class="panel">
        <h2>Batches and their files</h2>
        <p class="sub">Files are profiled (format, columns, record count, sha256) before they land in <code>raw/</code>. An identical file is detected by checksum and never loaded twice.</p>
        ${p.batches.map((b) => `
          <details ${b.label === "batch_0003_cdc" ? "open" : ""} style="margin-top:12px">
            <summary><b style="color:var(--text)">${esc(b.label)}</b> <span class="mono faint">${esc(b.batch_id)}</span> ${statusChip(b.status)}
              <span class="muted">${int(b.file_count)} files, ${int(b.total_records)} records, ${bytes(b.total_bytes)}</span></summary>
            ${table(b.files, [
              { key: "filename", label: "File" },
              { key: "entity", label: "Entity" },
              { key: "format", label: "Format" },
              { key: "record_count", label: "Records", num: true, fmt: int },
              { key: "size_bytes", label: "Size", num: true, fmt: bytes },
              { key: "bronze_rows", label: "Bronze rows", num: true, fmt: int },
              { key: "status", label: "Status", fmt: (v, r) => statusChip(v) + (r.error ? `<br><span class="faint">${esc(r.error)}</span>` : "") },
            ])}
          </details>`).join("")}
      </section>

      <section class="panel">
        <div class="row" style="justify-content:space-between">
          <div><h2>Stage runs</h2><p class="sub">One row per dataset per stage run. Silver rows show how the increment was read from Bronze.</p></div>
          <label class="row muted">Stage <select id="stage-filter"><option value="">all</option>${stages.map((s) => `<option>${esc(s)}</option>`).join("")}</select></label>
        </div>
        <div id="runs-table"></div>
      </section>

      <div class="grid g2">
        <section class="panel">
          <h2>Watermarks</h2>
          <p class="sub">Silver stores the last Bronze snapshot it consumed; Gold stores the last Silver change time.</p>
          ${table(p.watermarks, [
            { key: "pipeline", label: "Consumer" }, { key: "dataset", label: "Dataset" },
            { key: "value", label: "Watermark", wrap: true, fmt: (v) => `<code>${esc(v)}</code>` },
          ], { scroll: true })}
        </section>
        <section class="panel">
          <h2>Maintenance</h2>
          <p class="sub">Compaction, manifest rewrite, snapshot expiry and orphan cleanup per table.</p>
          ${table(p.maintenance.filter((m) => m.metrics && m.metrics.files_before > 0), [
            { key: "table_name", label: "Table" },
            { key: "metrics", label: "Data files", num: true, fmt: (m) => `${int(m.files_before)} → ${int(m.files_after)}` },
            { key: "duration_s", label: "Seconds", num: true, fmt: (v) => dec(v, 1) },
          ], { scroll: true, empty: "Maintenance hasn't run yet." })}
        </section>
      </div>`;
    const render = () => {
      const s = document.getElementById("stage-filter").value;
      const rows = p.runs.filter((r) => !s || r.stage === s);
      document.getElementById("runs-table").innerHTML = table(rows, [
        { key: "started_at", label: "Started", fmt: when },
        { key: "stage", label: "Stage" },
        { key: "dataset", label: "Dataset" },
        { key: "input_records", label: "In", num: true, fmt: int },
        { key: "output_records", label: "Out", num: true, fmt: int },
        { key: "quarantined_records", label: "Quarantined", num: true, fmt: (v) => (v ? `<span style="color:var(--quarantine)">${int(v)}</span>` : "0") },
        { key: "duration_s", label: "Seconds", num: true, fmt: (v) => dec(v, 1) },
        { key: "details", label: "Detail", wrap: true, fmt: (d) => runDetail(d) },
        { key: "status", label: "Status", fmt: statusChip },
      ], { scroll: true });
    };
    document.getElementById("stage-filter").addEventListener("change", render);
    render();
  }
  function runDetail(d) {
    if (!d || typeof d !== "object") return "";
    const bits = [];
    if (d.read_mode) bits.push(`read ${d.read_mode}`);
    if (d.inserted !== undefined) bits.push(`${int(d.inserted)} inserted, ${int(d.updated)} updated`);
    if (d.duplicates_removed) bits.push(`${int(d.duplicates_removed)} duplicates removed`);
    if (d.evolved_columns && d.evolved_columns.length) bits.push(`schema +${d.evolved_columns.join(", ")}`);
    if (d.rejected_files && d.rejected_files.length) bits.push(`<span style="color:var(--quarantine)">${d.rejected_files.length} file rejected by contract</span>`);
    if (d.months_recomputed) bits.push(`${d.months_recomputed.length} months recomputed`);
    if (d.affected_orders !== undefined) bits.push(`${int(d.affected_orders)} affected orders (${esc(d.mode)})`);
    if (d.reason) bits.push(esc(d.reason));
    if (d.file) bits.push(`<span class="faint">${esc(String(d.file).split("/").pop())}</span>`);
    if (d.files_before !== undefined) bits.push(`files ${int(d.files_before)} → ${int(d.files_after)}`);
    return bits.join("<br>");
  }

  // =============================================================== QUALITY
  async function pageQuality() {
    const q = await api("api/quality");
    const latest = {};
    q.reports.forEach((r) => { latest[r.dataset] = latest[r.dataset] || { checked: 0, invalid: 0, dup: 0, runs: 0 };
      const L = latest[r.dataset]; L.checked += r.total_records; L.invalid += r.invalid_records; L.dup += r.duplicate_records; L.runs++; });
    const failing = q.rules.filter((r) => r.failed > 0);
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Data quality</h1>
        <p class="page-lede">Rules are declared per entity in <code>config/pipeline_config.yaml</code> and compiled to SQL.
        Error-level failures move the record to <code>quarantine.&lt;entity&gt;</code> with the reasons attached; warnings are counted, never dropped.</p>
      </div></div>

      <div class="grid g2">
        <section class="panel">
          <h2>By quality dimension</h2>
          <p class="sub">All Silver runs. Failure rate is failed checks over records checked.</p>
          ${table(q.dimensions, [
            { key: "dimension", label: "Dimension" },
            { key: "rules", label: "Rules", num: true },
            { key: "checked", label: "Checked", num: true, fmt: int },
            { key: "failed", label: "Failed", num: true, fmt: (v) => (v ? `<b style="color:var(--quarantine)">${int(v)}</b>` : "0") },
            { key: "failed", label: "Pass rate", num: true, fmt: (v, r) => `${(100 - (100 * v) / Math.max(r.checked, 1)).toFixed(3)}%` },
          ])}
        </section>
        <section class="panel">
          <h2>Schema changes</h2>
          <p class="sub">Detected by comparing each file's header with the contract.</p>
          ${table(q.schema_changes, [
            { key: "dataset", label: "Dataset" },
            { key: "change_type", label: "Change", fmt: (v) => esc(String(v).toLowerCase().replace(/_/g, " ")) },
            { key: "column_name", label: "Column", fmt: (v) => `<code>${esc(v)}</code>` },
            { key: "action", label: "Decision", fmt: statusChip },
          ], { empty: "No schema drift detected." })}
        </section>
      </div>

      <section class="panel">
        <h2>Datasets</h2>
        <p class="sub">Totals across every Silver run for each entity.</p>
        ${table(Object.entries(latest).map(([k, v]) => ({ dataset: k, ...v })), [
          { key: "dataset", label: "Dataset" },
          { key: "runs", label: "Runs", num: true },
          { key: "checked", label: "Records checked", num: true, fmt: int },
          { key: "invalid", label: "Quarantined", num: true, fmt: int },
          { key: "dup", label: "Duplicates removed", num: true, fmt: int },
          { key: "checked", label: "Valid share", fmt: (v, r) => { const p = 1 - r.invalid / Math.max(v, 1);
            return `<div class="row" style="flex-wrap:nowrap"><div class="bar" style="--c:var(--ok);width:120px"><i style="width:${(p * 100).toFixed(2)}%"></i></div><span class="num">${(p * 100).toFixed(2)}%</span></div>`; } },
        ])}
      </section>

      <section class="panel">
        <h2>Rules that caught something</h2>
        <p class="sub">${failing.length} of ${q.rules.length} rules fired at least once. The rest passed on every record.</p>
        ${table(failing, [
          { key: "dataset", label: "Dataset" }, { key: "rule", label: "Rule", fmt: (v) => `<code>${esc(v)}</code>` },
          { key: "dimension", label: "Dimension" },
          { key: "severity", label: "Severity", fmt: (v) => chip(v, v === "error" ? "bad" : "warn") },
          { key: "checked", label: "Checked", num: true, fmt: int },
          { key: "failed", label: "Failed", num: true, fmt: int },
        ], { scroll: true })}
      </section>

      <section class="section">
        <h2>Quarantine</h2>
        <p class="sub">Original values are kept as text so a fix upstream can be replayed. The rule names explain each rejection.</p>
        <div class="grid g2">
        ${q.quarantine.map((t) => `
          <section class="panel" style="margin:0">
            <h3>${dot("quarantine")} quarantine.${esc(t.table)} <span class="muted num">${int(t.rows)} rows</span></h3>
            <div class="row" style="margin:10px 0">${t.rules.map((r) => chip(`${r.rule} × ${int(r.n)}`, "bad")).join(" ")}</div>
            <details><summary>Sample rows</summary>${autoTable(t.sample)}</details>
          </section>`).join("")}
        </div>
      </section>`;
  }

  // ============================================================== EXPLORER
  async function pageExplorer(params) {
    const tables = await api("api/tables");
    const [nsArg, nameArg, snapArg] = params;
    const pick = tables.find((t) => t.namespace === nsArg && t.table === nameArg) ||
      tables.find((t) => t.namespace === "silver" && t.table === "orders") || tables[0];
    const groups = LAYER_ORDER.map((l) => [l, tables.filter((t) => t.layer === l)]).filter(([, ts]) => ts.length);
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Table explorer</h1>
        <p class="page-lede">Schema, partitioning, snapshot history and data of every table in the catalog.
        Pick a snapshot to read the table as it was then${MODE === "live" ? " (a live DuckDB time-travel query)" : ""}.</p>
      </div></div>
      <div class="explorer">
        <nav class="tlist" aria-label="Tables">${groups.map(([l, ts]) => `
          <h3>${dot(l)} ${esc(l)}</h3>
          ${ts.map((t) => `<button class="layer-${esc(l)}" data-ns="${esc(t.namespace)}" data-t="${esc(t.table)}" aria-current="${t === pick}">${esc(t.table)}<span class="num">${int(t.rows)}</span></button>`).join("")}`).join("")}
        </nav>
        <div id="tdetail"><p class="loading">Loading table…</p></div>
      </div>`;
    view.querySelectorAll(".tlist button").forEach((b) => b.addEventListener("click", () => {
      location.hash = `#/explorer/${b.dataset.ns}/${b.dataset.t}`;
    }));
    if (MODE === "static" && pick.layer === "lab") {
      document.getElementById("tdetail").innerHTML = `<div class="notice">Lab tables are scratch copies rebuilt by every showcase run, so they aren't in the static snapshot. Their story is on the Iceberg lab page.</div>`;
      return;
    }
    try {
      const d = await api(`api/tables/${pick.namespace}/${pick.table}`);
      renderTable(d, snapArg);
    } catch (e) { document.getElementById("tdetail").innerHTML = errorBox(e, `${pick.namespace}.${pick.table}`); }
  }

  function renderTable(d, snapArg) {
    const host = document.getElementById("tdetail");
    const snaps = d.snapshots.slice().sort((a, b) => a.ts - b.ts);
    const refs = Object.entries(d.refs || {});
    const props = Object.entries(d.properties || {}).filter(([k]) => k !== "comment");
    host.innerHTML = `
      <section class="panel layer-${esc(d.layer)}">
        <h2>${dot(d.layer)} ${esc(d.namespace)}.${esc(d.table)}</h2>
        <p class="sub">${esc(d.comment || "")}</p>
        <div class="facts">
          <div><b class="num">${int(d.rows)}</b><span>rows</span></div>
          <div><b class="num">${int(d.data_files)}</b><span>data files</span></div>
          <div><b class="num">${bytes(d.bytes)}</b><span>on disk</span></div>
          <div><b class="num">${int(snaps.length)}</b><span>snapshots</span></div>
          <div><b class="num">${int(d.schema_versions)}</b><span>schema versions</span></div>
          <div><b>${d.partitioning.length ? esc(d.partitioning.join(", ")) : "none"}</b><span>partitioning</span></div>
        </div>
        <p class="faint mono" style="margin:10px 0 0;font-size:12px;overflow-wrap:anywhere">${esc(d.metadata_location)}</p>
      </section>

      <section class="panel">
        <h2>Snapshots</h2>
        <p class="sub">Oldest on the left. Filled node is the current snapshot${refs.length ? `; refs: ${refs.map(([n, r]) => `<code>${esc(n)}</code> (${esc(r.type)})`).join(", ")}` : ""}.</p>
        <div class="timeline" role="group" aria-label="Snapshots">
          ${snaps.map((s) => `<button class="snap layer-${esc(d.layer)} ${String(s.snapshot_id) === String(d.current_snapshot_id) ? "current" : ""}" data-id="${esc(s.snapshot_id)}" aria-pressed="false">
            <span class="node"></span>
            <span>${esc(s.operation)}</span>
            <small>${when(s.ts).slice(5, 16)}<br>+${int(s.summary["added-records"] || 0)}${s.summary["deleted-records"] ? ` / −${int(s.summary["deleted-records"])}` : ""}</small>
          </button>`).join("")}
        </div>
        <div id="snap-view" style="margin-top:12px"></div>
      </section>

      <div class="grid g2">
        <section class="panel">
          <h2>Schema</h2>
          <p class="sub">Iceberg tracks columns by field ID, which is why renames and reorders are safe.</p>
          ${table(d.schema, [
            { key: "id", label: "ID", num: true }, { key: "name", label: "Column", fmt: (v) => `<code>${esc(v)}</code>` },
            { key: "type", label: "Type" }, { key: "required", label: "Required", fmt: (v) => (v ? "yes" : "") },
          ], { scroll: true })}
        </section>
        <section class="panel">
          <h2>Schema and partition history</h2>
          ${d.schema_evolution.length ? d.schema_evolution.map((e) => `<p style="margin:8px 0 4px"><b>Schema ${esc(e.schema_id)}</b></p>
            ${e.changes.map((c) => `<div>${chip(c.change, c.change === "dropped" ? "bad" : c.change === "added" ? "ok" : "info")} <code>${esc(c.column)}</code>
              ${c.from ? `<span class="muted">from <code>${esc(c.from)}</code></span>` : ""} ${c.type ? `<span class="muted">${esc(c.type)}</span>` : ""}</div>`).join("") || `<span class="muted">metadata-only change</span>`}`).join("")
            : `<p class="muted">One schema version so far.</p>`}
          <p style="margin:16px 0 4px"><b>Partition specs</b></p>
          ${d.specs.map((s) => `<div>${chip("spec " + s.spec_id, s.spec_id === d.default_spec_id ? "ok" : "")} ${s.fields.length ? s.fields.map((f) => `<code>${esc(f)}</code>`).join(", ") : '<span class="muted">unpartitioned</span>'}</div>`).join("")}
          <details style="margin-top:16px"><summary>Table properties (${props.length})</summary>
            <dl class="kv">${props.map(([k, v]) => `<dt>${esc(k)}</dt><dd><code>${esc(v)}</code></dd>`).join("")}</dl></details>
        </section>
      </div>

      <section class="panel">
        <h2>Current data</h2>
        ${autoTable(d.sample)}
      </section>`;
    const buttons = host.querySelectorAll(".snap");
    const select = async (btn) => {
      buttons.forEach((b) => b.setAttribute("aria-pressed", String(b === btn)));
      const id = btn.dataset.id; const out = document.getElementById("snap-view");
      const s = snaps.find((x) => String(x.snapshot_id) === id);
      out.innerHTML = `<p class="muted">Reading snapshot ${esc(id)}…</p>`;
      const meta = `<dl class="kv"><dt>Snapshot</dt><dd class="mono">${esc(id)}</dd><dt>Committed</dt><dd>${when(s.ts)}</dd>
        <dt>Operation</dt><dd>${esc(s.operation)}</dd><dt>Records</dt><dd>${int(s.summary["total-records"])} total, +${int(s.summary["added-records"] || 0)} added, −${int(s.summary["deleted-records"] || 0)} deleted</dd>
        <dt>Data files</dt><dd>${int(s.summary["total-data-files"])} total, +${int(s.summary["added-data-files"] || 0)} added</dd></dl>`;
      try {
        const p = await api(`api/tables/${d.namespace}/${d.table}/snapshots/${id}`);
        out.innerHTML = `${meta}<pre class="sql">${highlightSql(p.sql)}</pre>
          <p class="muted" style="margin:8px 0">${int(p.rows)} rows at this snapshot.</p>${autoTable(p.sample)}`;
      } catch (e) {
        out.innerHTML = meta + (MODE === "static" ? `<p class="muted">Row previews are exported for Silver and Gold tables only.</p>` : errorBox(e, "the snapshot"));
      }
    };
    buttons.forEach((b) => b.addEventListener("click", () => select(b)));
    const initial = [...buttons].find((b) => b.dataset.id === String(snapArg)) || [...buttons].find((b) => b.classList.contains("current"));
    if (initial) { select(initial); initial.scrollIntoView({ block: "nearest", inline: "center" }); }
  }

  // =================================================================== LAB
  async function pageLab() {
    const lab = await api("api/lab");
    const demos = lab.demos || [];
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Iceberg lab</h1>
        <p class="page-lede">Seven experiments that show what makes this a lakehouse rather than folders of Parquet files.
        Each runs on scratch copies in the <code>lab</code> namespace; every number below was measured during the run${lab.generated_at ? ` of ${esc(lab.generated_at)} UTC` : ""}.</p>
      </div></div>
      <nav class="row" aria-label="Experiments">${demos.map((d) => `<a class="chip ${d.status === "ok" ? "ok" : "bad"}" href="#/lab/${esc(d.id)}">${esc(d.title)}</a>`).join("")}</nav>
      ${demos.length ? demos.map(renderDemo).join("") : `<div class="notice" style="margin-top:20px">The lab hasn't run yet. Run <code>docker compose run --rm openlake python3 -m openlake showcase</code>.</div>`}`;
  }
  function renderDemo(d) {
    const metrics = Object.entries(d.metrics || {}).filter(([, v]) => v !== null && v !== undefined);
    return `<article class="demo" id="demo-${esc(d.id)}">
      <div class="demo-head">
        <div><h2>${esc(d.title)}</h2><p>${esc(d.summary || "")}</p></div>
        <div>${d.status === "ok" ? chip(`passed in ${d.seconds}s`, "ok") : chip("failed", "bad")}</div>
      </div>
      ${d.why ? `<p class="why">${esc(d.why)}</p>` : ""}
      ${d.error ? `<div class="notice error">${esc(d.error)}</div>` : ""}
      ${metrics.length ? `<div class="facts" style="margin-top:14px">${metrics.map(([k, v]) => `<div><b class="num">${typeof v === "boolean" ? (v ? "yes" : "no") : typeof v === "number" ? (Number.isInteger(v) ? int(v) : dec(v, 1)) : esc(v)}</b><span>${esc(k.replace(/_/g, " "))}</span></div>`).join("")}</div>` : ""}
      <ol class="steps">${(d.steps || []).map((s) => `<li>
        <div class="label">${esc(s.label)}${s.ms ? ` <span class="faint num">${ms(s.ms)}</span>` : ""}</div>
        <div class="result">${s.sql ? `<pre class="sql">${highlightSql(s.sql)}</pre>` : ""}${renderResult(s.result)}</div>
      </li>`).join("")}</ol>
    </article>`;
  }
  function renderResult(r) {
    if (r === null || r === undefined) return "";
    if (Array.isArray(r)) {
      if (!r.length) return `<p class="muted">(no rows)</p>`;
      if (typeof r[0] === "object") return `<div style="margin-top:8px">${autoTable(r)}</div>`;
      return `<div class="row" style="margin-top:8px">${r.map((x) => `<code>${esc(x)}</code>`).join(" ")}</div>`;
    }
    if (typeof r === "object") {
      return `<dl class="kv">${Object.entries(r).map(([k, v]) => `<dt>${esc(k.replace(/_/g, " "))}</dt><dd>${
        Array.isArray(v) ? (v.length && typeof v[0] === "object" ? autoTable(v) : v.length ? v.map((x) => `<code>${esc(String(x).split("/").slice(-2).join("/"))}</code>`).join("<br>") : "none")
          : typeof v === "object" && v !== null ? `<code>${esc(JSON.stringify(v))}</code>` : typeof v === "boolean" ? (v ? chip("yes", "ok") : chip("no", "bad")) : typeof v === "number" ? int(v) : esc(v)}</dd>`).join("")}</dl>`;
    }
    return `<p>${esc(r)}</p>`;
  }

  // ============================================================= ANALYTICS
  async function pageAnalytics() {
    const a = await api("api/analytics");
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Business analytics</h1>
        <p class="page-lede">What the Gold layer is for. Every chart is a DuckDB query over Gold Iceberg tables: the star schema
        (<code>fact_orders</code>, <code>fact_order_items</code>, <code>fact_payments</code>, four dimensions) and the marts built from it.</p>
      </div></div>
      <section class="panel">
        <h2>Monthly revenue and orders</h2>
        <p class="sub">From <code>gold.daily_revenue</code>, which Gold refreshes month by month with a dynamic partition overwrite.
        September 2018 holds only the two change batches, so it is a partial month.</p>
        <div class="chart-box tall"><canvas id="c-monthly"></canvas></div>
      </section>
      <div class="grid g2">
        <section class="panel"><h2>Top categories</h2><p class="sub">Revenue by English category name (the Portuguese names are translated in <code>dim_products</code>).</p>
          <div class="chart-box tall"><canvas id="c-cat"></canvas></div></section>
        <section class="panel"><h2>Late deliveries hurt reviews</h2><p class="sub">Average review score by delivery time against the estimate.</p>
          <div class="chart-box tall"><canvas id="c-delay"></canvas></div></section>
      </div>
      <div class="grid g2">
        <section class="panel"><h2>Delivery time by state</h2><p class="sub">The further from São Paulo, where most sellers are, the longer the wait.</p>
          <div class="chart-box tall"><canvas id="c-state"></canvas></div></section>
        <section class="panel"><h2>Payment methods</h2><p class="sub">Credit card dominates; boleto (bank slip) is second.</p>
          <div class="chart-box tall"><canvas id="c-pay"></canvas></div></section>
      </div>
      <div class="grid g-wide">
        <section class="panel"><h2>Top sellers</h2>${table(a.sellers, [
          { key: "revenue_rank", label: "#", num: true }, { key: "seller_id", label: "Seller", fmt: (v) => `<span class="mono">${esc(String(v).slice(0, 10))}…</span>` },
          { key: "city", label: "City" }, { key: "state", label: "State" }, { key: "orders", label: "Orders", num: true, fmt: int },
          { key: "revenue", label: "Revenue", num: true, fmt: brl }, { key: "avg_review_score", label: "Review", num: true, fmt: (v) => dec(v, 2) },
          { key: "late_delivery_rate", label: "Late", num: true, fmt: (v) => pct(v) },
        ])}</section>
        <section class="panel"><h2>A column that arrived later</h2>
          <p class="sub"><code>delivery_priority</code> appeared in batch 2. Schema evolution added it to Silver and Gold; older orders read it as null.</p>
          ${table(a.priority, [{ key: "priority", label: "Priority" }, { key: "orders", label: "Orders", num: true, fmt: int },
            { key: "avg_delivery_days", label: "Avg days", num: true, fmt: (v) => dec(v, 1) }])}</section>
      </div>`;
    const m = a.monthly;
    chart("c-monthly", {
      data: { labels: m.map((r) => r.month), datasets: [
        { type: "bar", label: "Revenue (R$)", data: m.map((r) => r.revenue), backgroundColor: LAYER_COLOR.gold, borderRadius: 3, yAxisID: "y" },
        { type: "line", label: "Orders", data: m.map((r) => r.orders), borderColor: LAYER_COLOR.ops, backgroundColor: LAYER_COLOR.ops, pointRadius: 2, tension: 0.25, yAxisID: "y1" },
      ] },
      options: { maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        scales: { y: { beginAtZero: true, ticks: { callback: (v) => brlShort(v) } }, y1: { beginAtZero: true, position: "right", grid: { display: false }, title: { display: true, text: "orders" } }, x: { grid: { display: false } } } },
    });
    const cats = a.categories.slice(0, 12);
    chart("c-cat", { type: "bar", data: { labels: cats.map((r) => r.category.replace(/_/g, " ")), datasets: [{ data: cats.map((r) => r.revenue), backgroundColor: LAYER_COLOR.gold, borderRadius: 3 }] },
      options: { indexAxis: "y", maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => brl(c.raw) } } },
        scales: { x: { ticks: { callback: (v) => brlShort(v) } }, y: { grid: { display: false } } } } });
    const order = ["10+ days early", "3-10 days early", "on time", "1-5 days late", "5+ days late", "not delivered"];
    const delay = order.map((b) => a.review_vs_delay.find((r) => r.bucket === b)).filter(Boolean);
    chart("c-delay", { type: "bar", data: { labels: delay.map((r) => r.bucket), datasets: [{ data: delay.map((r) => r.avg_review),
      backgroundColor: delay.map((r) => (/late|not/.test(r.bucket) ? LAYER_COLOR.quarantine : LAYER_COLOR.silver)), borderRadius: 3 }] },
      options: { maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${c.raw} average, ${int(delay[c.dataIndex].orders)} orders` } } },
        scales: { y: { min: 1, max: 5, title: { display: true, text: "average review (1–5)" } }, x: { grid: { display: false } } } } });
    const st = a.states.slice().sort((x, y) => x.avg_delivery_days - y.avg_delivery_days);
    chart("c-state", { type: "bar", data: { labels: st.map((r) => r.state), datasets: [{ label: "Average delivery days", data: st.map((r) => r.avg_delivery_days), backgroundColor: LAYER_COLOR.bronze, borderRadius: 2 }] },
      options: { maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => { const r = st[c.dataIndex]; return [`${dec(r.avg_delivery_days, 1)} days on average`, `${pct(r.late_delivery_rate)} late`, `${int(r.orders)} orders (${r.region})`]; } } } },
        scales: { x: { grid: { display: false } }, y: { title: { display: true, text: "days" } } } } });
    chart("c-pay", { type: "bar", data: { labels: a.payments.map((r) => r.payment_type.replace(/_/g, " ")), datasets: [{ label: "Value (R$)", data: a.payments.map((r) => r.value), backgroundColor: [LAYER_COLOR.gold, LAYER_COLOR.silver, LAYER_COLOR.bronze, LAYER_COLOR.ops, LAYER_COLOR.lab], borderRadius: 3 }] },
      options: { maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => { const r = a.payments[c.dataIndex]; return [brl(r.value), `${int(r.transactions)} transactions`, `${dec(r.avg_installments, 1)} installments on average`]; } } } },
        scales: { y: { ticks: { callback: (v) => brlShort(v) } }, x: { grid: { display: false } } } } });
  }

  // ============================================================ BENCHMARKS
  async function pageBenchmarks() {
    const b = await api("api/benchmarks");
    if (!b || !b.queries) { view.innerHTML = `<h1 class="page-title">Benchmarks</h1><div class="notice" style="margin-top:20px">No benchmark results yet. Run <code>python3 -m openlake benchmark</code>.</div>`; return; }
    const layers = ["raw_csv", "raw_parquet", "silver_iceberg", "gold_iceberg"];
    const names = { raw_csv: "Raw CSV", raw_parquet: "Raw Parquet", silver_iceberg: "Silver (Iceberg)", gold_iceberg: "Gold (Iceberg)" };
    const colors = { raw_csv: "#627C8A", raw_parquet: LAYER_COLOR.ops, silver_iceberg: LAYER_COLOR.silver, gold_iceberg: LAYER_COLOR.gold };
    const queries = [...new Map(b.queries.map((q) => [q.query, q.title])).entries()];
    const env = b.environment || {};
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">Benchmarks</h1>
        <p class="page-lede">The same questions asked of each layer, on the same machine. Measured ${esc(String(b.generated_at).replace("T", " "))};
        median of ${esc(b.repeats)} runs after a warm-up. Spark ${esc(env.spark)}, DuckDB ${esc(env.duckdb)}, Iceberg ${esc(env.iceberg)}, ${esc(env.host_cpus)} CPUs.</p>
      </div></div>
      <div class="grid g3">${queries.map(([id, title]) => `<section class="panel" style="margin:0"><h3>${esc(title)}</h3>
        <p class="sub">DuckDB latency, lower is better.</p><div class="chart-box"><canvas id="b-${esc(id)}"></canvas></div></section>`).join("")}</div>
      <section class="panel">
        <h2>All measurements</h2>
        <p class="sub">"Eligible" is what the engine must open: whole files for raw data, and data files left after partition pruning for Iceberg tables.</p>
        ${table(b.queries, [
          { key: "title", label: "Query", wrap: true }, { key: "layer", label: "Layer", fmt: (v) => esc(names[v]) }, { key: "engine", label: "Engine" },
          { key: "median_ms", label: "Median", num: true, fmt: ms }, { key: "eligible_files", label: "Files", num: true, fmt: int },
          { key: "eligible_bytes", label: "Bytes", num: true, fmt: bytes }, { key: "result_rows", label: "Result rows", num: true, fmt: int },
        ], { scroll: true })}
      </section>
      <div class="grid g2">
        <section class="panel"><h2>Storage</h2><p class="sub">Same entities in each format. Iceberg files are zstd-compressed Parquet.</p>
          ${table(b.storage, [{ key: "dataset", label: "Dataset" }, { key: "raw_csv_bytes", label: "Raw CSV", num: true, fmt: bytes },
            { key: "raw_parquet_bytes", label: "Raw Parquet", num: true, fmt: bytes }, { key: "silver_iceberg_bytes", label: "Silver Iceberg", num: true, fmt: bytes },
            { key: "silver_rows", label: "Silver rows", num: true, fmt: int }])}</section>
        <section class="panel"><h2>Compaction</h2><p class="sub">40 micro-batch appends, then <code>rewrite_data_files</code>, <code>rewrite_manifests</code> and <code>expire_snapshots</code>.</p>
          ${b.compaction ? table([
            { m: "Data files", a: b.compaction.files_before, z: b.compaction.files_after },
            { m: "Manifests", a: b.compaction.manifests_before, z: b.compaction.manifests_after },
            { m: "Snapshots", a: b.compaction.snapshots_before, z: b.compaction.snapshots_after },
            { m: "Aggregation query (Spark)", a: ms(b.compaction.query_ms_before), z: ms(b.compaction.query_ms_after) },
          ], [{ key: "m", label: "" }, { key: "a", label: "Before", num: true, fmt: (v) => esc(v) }, { key: "z", label: "After", num: true, fmt: (v) => esc(v) }]) : '<p class="muted">Not measured.</p>'}</section>
      </div>
      <section class="panel"><h2>Pipeline throughput</h2>
        ${table(b.pipeline, [{ key: "stage", label: "Stage" }, { key: "started_at", label: "Started", fmt: when }, { key: "seconds", label: "Seconds", num: true, fmt: (v) => dec(v, 1) },
          { key: "input_records", label: "Input", num: true, fmt: int }, { key: "output_records", label: "Output", num: true, fmt: int },
          { key: "records_per_second", label: "Records/s", num: true, fmt: int }])}</section>
      <section class="panel"><h2>How to read this</h2><p class="sub" style="white-space:pre-line;max-width:90ch">${esc(b.methodology)}</p></section>`;
    queries.forEach(([id]) => {
      const rows = layers.map((l) => b.queries.find((q) => q.query === id && q.layer === l && q.engine === "duckdb")).filter(Boolean);
      chart(`b-${id}`, { type: "bar", data: { labels: rows.map((r) => names[r.layer]), datasets: [{ data: rows.map((r) => r.median_ms), backgroundColor: rows.map((r) => colors[r.layer]), borderRadius: 3 }] },
        options: { maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => { const r = rows[c.dataIndex]; return [`${ms(r.median_ms)} median`, `${int(r.eligible_files)} files, ${bytes(r.eligible_bytes)}`]; } } } },
          scales: { y: { title: { display: true, text: "ms" } }, x: { grid: { display: false } } } } });
    });
  }

  // =================================================================== SQL
  async function pageSql() {
    const examples = await api("api/sql/examples");
    const live = MODE === "live";
    view.innerHTML = `
      <div class="page-head"><div>
        <h1 class="page-title">SQL console</h1>
        <p class="page-lede">Read-only DuckDB, attached to the Iceberg REST catalog as <code>lake</code>. Try
        <code>SELECT * FROM lake.silver.orders AT (VERSION =&gt; &lt;snapshot id&gt;)</code> for time travel.</p>
      </div></div>
      ${live ? "" : `<div class="notice" style="margin-bottom:18px">This is a static snapshot, so queries can't run here. The example results below were saved from a real run. Start the stack with <code>docker compose up</code> to query live.</div>`}
      <div class="sql-layout">
        <section class="panel examples" style="margin:0"><h3 style="margin-bottom:8px">Examples</h3>
          ${examples.map((e, i) => `<button data-i="${i}" aria-pressed="false">${esc(e.title)}</button>`).join("")}</section>
        <section style="min-width:0">
          <label for="sql-editor" class="muted" style="font-size:13px">Query</label>
          <textarea id="sql-editor" class="editor" spellcheck="false" ${live ? "" : "readonly"}>SELECT order_status, count(*) AS orders
FROM lake.silver.orders
GROUP BY 1 ORDER BY 2 DESC;</textarea>
          <div class="row" style="margin:10px 0 16px">
            <button id="sql-run" class="btn" ${live ? "" : "disabled"}>Run query</button>
            <span class="muted" style="font-size:13px">${live ? "Ctrl + Enter runs it. Results are capped at 500 rows." : ""}</span>
            <span id="sql-meta" class="muted num" style="margin-left:auto;font-size:13px"></span>
          </div>
          <div id="sql-out"></div>
        </section>
      </div>`;
    const ed = document.getElementById("sql-editor"); const out = document.getElementById("sql-out"); const meta = document.getElementById("sql-meta");
    const show = (res) => {
      if (res.error) { out.innerHTML = `<div class="notice error">${esc(res.error)}</div>`; meta.textContent = ""; return; }
      const rows = res.rows.map((r) => Object.fromEntries(res.columns.map((c, i) => [c, r[i]])));
      out.innerHTML = autoTable(rows, { scroll: true });
      meta.textContent = `${int(res.rows.length)} rows${res.truncated ? " (truncated)" : ""} in ${ms(res.ms)}`;
    };
    const run = async () => {
      if (!live) return;
      const btn = document.getElementById("sql-run"); btn.disabled = true; meta.textContent = "Running…";
      try {
        const r = await fetch("api/sql", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sql: ed.value }) });
        const j = await r.json();
        show(r.ok ? j : { error: j.detail || "Query failed." });
      } catch (e) { show({ error: e.message }); } finally { btn.disabled = false; }
    };
    document.getElementById("sql-run").addEventListener("click", run);
    ed.addEventListener("keydown", (e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); run(); } });
    view.querySelectorAll(".examples button").forEach((b) => b.addEventListener("click", () => {
      view.querySelectorAll(".examples button").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      const ex = examples[Number(b.dataset.i)]; ed.value = ex.sql;
      if (live) run(); else if (ex.result) show(ex.result);
    }));
    if (!live && examples.length) view.querySelector(".examples button").click();
  }

  // ================================================================ ROUTER
  const PAGES = { overview: pageOverview, pipeline: pagePipeline, quality: pageQuality, explorer: pageExplorer,
    lab: pageLab, analytics: pageAnalytics, benchmarks: pageBenchmarks, sql: pageSql };
  const TITLES = { overview: "Overview", pipeline: "Pipeline", quality: "Data quality", explorer: "Table explorer",
    lab: "Iceberg lab", analytics: "Business analytics", benchmarks: "Benchmarks", sql: "SQL console" };

  async function route() {
    const parts = (location.hash.replace(/^#\/?/, "") || "overview").split("/").map(decodeURIComponent);
    const page = PAGES[parts[0]] ? parts[0] : "overview";
    document.querySelectorAll(".rail nav a").forEach((a) => {
      if (a.dataset.page === page) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
    });
    document.title = `${TITLES[page]} · OpenLake Engine`;
    destroyCharts(); hideTip();
    const sameExplorer = page === "explorer" && view.dataset.page === "explorer" && parts.length > 1;
    if (!sameExplorer) view.innerHTML = `<p class="loading">Loading ${esc(TITLES[page].toLowerCase())}…</p>`;
    view.dataset.page = page;
    try {
      await PAGES[page](parts.slice(1));
      if (page === "lab" && parts[1]) document.getElementById(`demo-${parts[1]}`)?.scrollIntoView();
      else if (!sameExplorer) window.scrollTo(0, 0);
    } catch (e) { view.innerHTML = errorBox(e, TITLES[page].toLowerCase()); }
  }

  window.addEventListener("hashchange", route);
  detectMode().then(route);
})();
