/* uric dashboard - vanilla JS, no external dependencies. */
(function () {
  "use strict";

  const state = {
    ric: null, nodes: [], ues: [], subs: [], xapps: [], events: [],
    nodeDetail: {}, history: {}, nodeSamples: {}, ueSamples: {}, live: false, ueFilter: "", ueNode: "", evFilter: "",
  };
  const HIST = 60;
  const view = document.getElementById("view");

  // ------------------------------------------------------------ helpers
  const esc = (v) => String(v == null ? "" : v).replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const fmtNum = (v, d = 1) => {
    if (v == null || v === "") return "–";
    if (typeof v !== "number") return esc(v);
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { maximumFractionDigits: d });
  };
  const ago = (ts) => {
    if (!ts) return "–";
    const s = Math.max(0, Date.now() / 1000 - ts);
    if (s < 2) return "now";
    if (s < 60) return Math.floor(s) + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  };
  const dur = (s) => {
    s = Math.floor(s || 0);
    const d = Math.floor(s / 86400), h = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60);
    return (d ? d + "d " : "") + (h ? h + "h " : "") + m + "m " + (s % 60) + "s";
  };
  const time = (ts) => new Date(ts * 1000).toLocaleTimeString();
  const hex = (n, w) => (n == null ? "–" : Number(n).toString(16).padStart(w || 1, "0"));
  const smBadge = (sm) => `<span class="badge ${sm === "KPM" ? "b-kpm" : sm === "RC" ? "b-rc" : "b-muted"}">${esc(sm)}</span>`;
  const stateBadge = (s) => {
    const cls = { active: "b-ok", running: "b-ok", pending: "b-warn", deleting: "b-warn", stopping: "b-warn",
      loaded: "b-accent", available: "b-muted", stopped: "b-muted", deleted: "b-muted", failed: "b-err" }[s] || "b-muted";
    return `<span class="badge ${cls}">${esc(s)}</span>`;
  };
  const cellStr = (c) => (c ? `${c.type.toUpperCase()} ${esc(c.plmn)}/${hex(c.nci != null ? c.nci : c.eci)}` : "–");
  const nodeLabel = (n) => {
    const i = n.info || {};
    return `${esc(i.type || "?")} ${esc(i.plmn || "")} id ${hex(i.nb_id)}`;
  };

  async function api(path, opts) {
    const r = await fetch("api/" + path, Object.assign({ headers: { "Content-Type": "application/json" } }, opts || {}));
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(body.error || r.statusText);
    return body;
  }
  function toast(msg, err) {
    const t = document.getElementById("toast");
    t.textContent = msg;
    t.className = "toast" + (err ? " err" : "");
    t.hidden = false;
    clearTimeout(toast._t);
    toast._t = setTimeout(() => (t.hidden = true), 3500);
  }
  function pushHist(key, v) {
    if (typeof v !== "number") return;
    const h = state.history[key] || (state.history[key] = []);
    h.push(v);
    if (h.length > HIST) h.shift();
  }
  // One sample per KPM report seen by the dashboard (the dashboard polls every 2 s, so the same
  // report can be fetched several times: samples are deduplicated on the report time).
  // ``extra`` holds per-sample identification columns (e.g. the serving node of a UE).
  function recordSample(store, key, ts, metrics, extra) {
    if (!ts || !Object.keys(metrics || {}).length) return;
    const samples = store[key] || (store[key] = []);
    if (samples.length && samples[samples.length - 1].ts === ts) return;
    samples.push({ ts, metrics: Object.assign({}, metrics), extra: extra || {} });
    if (samples.length > HIST) samples.shift();
  }

  function csvField(v) {
    if (v == null) return "";
    const s = typeof v === "object" ? JSON.stringify(v) : String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  // KPI samples as CSV: a header row (timestamp, unix_time, the ``idCols``, then every metric
  // seen), then one row per KPM report, oldest first. ``idCols`` maps a column name to its value
  // (a constant or a function of the sample).
  function kpisCsv(samples, idCols) {
    const metrics = [];
    for (const smp of samples) for (const k of Object.keys(smp.metrics)) if (!metrics.includes(k)) metrics.push(k);
    const ids = Object.entries(idCols);
    const rows = [["timestamp", "unix_time", ...ids.map(([name]) => name), ...metrics]];
    for (const smp of samples) {
      rows.push([new Date(smp.ts * 1000).toISOString(), smp.ts.toFixed(3),
        ...ids.map(([, v]) => (typeof v === "function" ? v(smp) : v)),
        ...metrics.map((k) => smp.metrics[k])]);
    }
    return rows.map((r) => r.map(csvField).join(",")).join("\r\n") + "\r\n";
  }

  function fileStamp() {
    return new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "").replace("T", "-");
  }

  function exportButton(act, attr, value, samples) {
    return `<button type="button" data-act="${act}" ${attr}="${esc(value)}" ${samples.length ? "" : "disabled"}
      title="Download the metrics of this card as CSV (one row per KPM report, up to the last ${HIST})">Export CSV</button>`;
  }

  function downloadFile(name, text, type) {
    const url = URL.createObjectURL(new Blob([text], { type }));
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function spark(key) {
    const h = state.history[key];
    if (!h || h.length < 2) return '<svg class="spark"></svg>';
    const w = 120, ht = 28, min = Math.min(...h), max = Math.max(...h), rng = max - min || 1;
    const pts = h.map((v, i) => [(i / (HIST - 1)) * w, ht - 2 - ((v - min) / rng) * (ht - 4)]);
    const off = w - pts[pts.length - 1][0];
    const p = pts.map(([x, y]) => `${(x + off).toFixed(1)},${y.toFixed(1)}`).join(" ");
    const area = `${(pts[0][0] + off).toFixed(1)},${ht} ${p} ${w},${ht}`;
    return `<svg class="spark" viewBox="0 0 ${w} ${ht}" preserveAspectRatio="none"><polygon class="area" points="${area}"/><polyline points="${p}"/></svg>`;
  }

  // ------------------------------------------------------------ data
  async function refresh() {
    try {
      const [ric, nodes, ues, subs, xapps] = await Promise.all([
        api("ric"), api("nodes"), api("ues"), api("subscriptions"), api("xapps")]);
      Object.assign(state, { ric, nodes, ues, subs, xapps });
      for (const n of nodes) for (const [k, v] of Object.entries(n.metrics || {})) pushHist(`n:${n.id}:${k}`, v);
      for (const n of nodes) recordSample(state.nodeSamples, n.id, n.metrics_ts, n.metrics);
      for (const u of ues) recordSample(state.ueSamples, u.key, u.metrics_ts, u.metrics, { node_id: u.node_id });
      for (const u of ues) for (const [k, v] of Object.entries(u.metrics || {})) pushHist(`u:${u.key}:${k}`, v);
      pushHist("ric:indications", ric.stats.indications);
      updateHeader();
      render();
    } catch (e) {
      setLive(false);
    }
  }
  function updateHeader() {
    const r = state.ric;
    if (!r) return;
    document.getElementById("c-nodes").textContent = r.nodes;
    document.getElementById("c-ues").textContent = r.ues;
    document.getElementById("c-subs").textContent = r.subscriptions;
    document.getElementById("c-xapps").textContent = r.xapps;
    const l = (r.listeners || []).map((x) => `${x.transport.toUpperCase()}:${x.port}`).join(" ");
    document.getElementById("ric-meta").textContent = `PLMN ${r.plmn} · RIC ID ${r.ric_id} · E2 ${l} · up ${dur(r.uptime)}`;
  }
  function setLive(on) {
    state.live = on;
    const el = document.getElementById("live");
    el.textContent = on ? "live" : "offline";
    el.className = "pill " + (on ? "pill-on" : "pill-off");
  }
  function connectEvents() {
    let es;
    try { es = new EventSource("api/events"); } catch (e) { return; }
    es.onopen = () => setLive(true);
    es.onerror = () => setLive(false);
    es.onmessage = (m) => {
      let ev;
      try { ev = JSON.parse(m.data); } catch (e) { return; }
      state.events.unshift(ev);
      if (state.events.length > 500) state.events.pop();
      if (/^(node|subscription|xapp|ue\.(added|removed))/.test(ev.event)) scheduleRefresh();
      else if (currentRoute()[0] === "events") render();
    };
  }
  let refreshTimer = null;
  function scheduleRefresh() {
    if (refreshTimer) return;
    refreshTimer = setTimeout(() => { refreshTimer = null; refresh(); }, 250);
  }

  // ------------------------------------------------------------ routing
  function currentRoute() {
    const h = location.hash.replace(/^#\/?/, "") || "overview";
    return h.split("/").map(decodeURIComponent);
  }
  window.addEventListener("hashchange", () => { render(true); });

  function render(navigated) {
    const [page, arg] = currentRoute();
    document.querySelectorAll("#tabs a").forEach((a) => a.classList.toggle("active", a.dataset.view === page));
    // do not re-render while the user types in a form
    const ae = document.activeElement;
    if (!navigated && ae && view.contains(ae) && /INPUT|TEXTAREA|SELECT/.test(ae.tagName)) return;
    const openDetails = new Set([...view.querySelectorAll("details[open][data-k]")].map((d) => d.dataset.k));
    const scroll = window.scrollY;
    let html;
    switch (page) {
      case "nodes": html = arg ? renderNode(arg) : renderNodes(); break;
      case "ues": html = arg ? renderUE(arg) : renderUEs(); break;
      case "subscriptions": html = renderSubs(); break;
      case "xapps": html = renderXApps(); break;
      case "events": html = renderEvents(); break;
      default: html = renderOverview();
    }
    view.innerHTML = html;
    view.querySelectorAll("details[data-k]").forEach((d) => { if (openDetails.has(d.dataset.k)) d.open = true; });
    if (!navigated) window.scrollTo(0, scroll); else window.scrollTo(0, 0);
    bind();
  }

  // ------------------------------------------------------------ views
  function renderOverview() {
    const r = state.ric || { stats: {} };
    const ind = state.history["ric:indications"] || [];
    const rate = ind.length > 1 ? Math.max(0, (ind[ind.length - 1] - ind[ind.length - 2]) / 2) : 0;
    const connected = state.nodes.filter((n) => n.connected);
    return `
    <div class="page-head"><div><h1>Overview</h1><div class="sub">Near-RT RIC status, E2 topology and recent activity</div></div></div>
    <div class="stats">
      ${stat("E2 nodes", connected.length, `${state.nodes.length - connected.length} disconnected`)}
      ${stat("UEs", state.ues.length, "tracked by the RIC")}
      ${stat("Subscriptions", r.subscriptions || 0, `${state.subs.length} total`)}
      ${stat("xApps running", r.xapps || 0, `${state.xapps.length} available`)}
      ${stat("Indications", fmtNum(r.stats.indications), `${fmtNum(rate)} / s`)}
      ${stat("Controls", fmtNum(r.stats.controls), `${fmtNum(r.stats.errors)} errors`)}
    </div>
    <div class="grid grid-2">
      <div class="panel"><div class="panel-head"><h2>E2 topology</h2><span class="muted">click a node or UE</span></div>
        <div class="panel-body flush">${topology()}</div></div>
      <div class="panel"><div class="panel-head"><h2>E2 nodes</h2><a href="#/nodes">all nodes →</a></div>
        <div class="panel-body flush table-wrap">${nodesTable(state.nodes)}</div></div>
    </div>
    <div class="panel" style="margin-top:16px"><div class="panel-head"><h2>Recent events</h2><a href="#/events">event log →</a></div>
      <div class="events short">${eventRows(state.events.slice(0, 40))}</div></div>`;
  }
  const stat = (l, v, h) => `<div class="panel stat"><div class="label">${esc(l)}</div><div class="value">${v}</div><div class="hint">${esc(h)}</div></div>`;

  function topology() {
    const W = 640, H = 420, cx = W / 2, cy = H / 2;
    const nodes = state.nodes;
    if (!nodes.length) return `<div class="empty">No E2 node connected yet.<br>Start one, e.g. <code>python -m uric.sim</code></div>`;
    const R = Math.min(W, H) / 2 - 70;
    let links = "", shapes = "";
    nodes.forEach((n, i) => {
      const a = (i / nodes.length) * Math.PI * 2 - Math.PI / 2;
      const x = cx + R * Math.cos(a), y = cy + R * Math.sin(a);
      links += `<line class="link" x1="${cx}" y1="${cy}" x2="${x}" y2="${y}"/>`;
      const ues = state.ues.filter((u) => u.node_id === n.id);
      ues.forEach((u, j) => {
        const b = a + ((j - (ues.length - 1) / 2) * 0.32);
        const r2 = 52 + (j % 2) * 10;
        const ux = x + r2 * Math.cos(b), uy = y + r2 * Math.sin(b);
        links += `<line class="link ue" x1="${x}" y1="${y}" x2="${ux}" y2="${uy}"/>`;
        shapes += `<a href="#/ues/${encodeURIComponent(u.key)}"><circle class="ue" cx="${ux}" cy="${uy}" r="5"><title>${esc(u.key)}</title></circle></a>`;
      });
      shapes += `<a href="#/nodes/${encodeURIComponent(n.id)}"><rect class="node ${n.connected ? "" : "off"}" x="${x - 22}" y="${y - 16}" width="44" height="32" rx="7"><title>${esc(n.id)}</title></rect>
        <text x="${x}" y="${y + 4}" text-anchor="middle">${esc((n.info && n.info.type) || "")}</text>
        <text class="muted" x="${x}" y="${y + 30}" text-anchor="middle">${esc(n.id)}</text></a>`;
    });
    shapes += `<circle class="ric" cx="${cx}" cy="${cy}" r="30"/><text class="inv" x="${cx}" y="${cy + 4}" text-anchor="middle">RIC</text>`;
    return `<svg class="topo" viewBox="0 0 ${W} ${H}">${links}${shapes}</svg>`;
  }

  function nodesTable(nodes) {
    if (!nodes.length) return `<div class="empty">No E2 nodes</div>`;
    return `<table><thead><tr><th>Node</th><th>Type</th><th>RAN functions</th><th class="num">UEs</th><th>Status</th></tr></thead><tbody>
      ${nodes.map((n) => `<tr class="clickable" data-href="#/nodes/${encodeURIComponent(n.id)}">
        <td class="mono">${esc(n.id)}</td><td>${nodeLabel(n)}</td>
        <td>${n.ran_functions.map((f) => smBadge(f.service_model)).join(" ")}</td>
        <td class="num">${n.ue_count}</td>
        <td>${n.connected ? '<span class="badge b-ok">connected</span>' : '<span class="badge b-err">disconnected</span>'}</td></tr>`).join("")}
      </tbody></table>`;
  }

  function renderNodes() {
    const nodes = state.nodes;
    return `<div class="page-head"><div><h1>E2 nodes</h1><div class="sub">Nodes that completed E2 Setup and the service models they expose</div></div></div>
    ${nodes.length ? `<div class="node-cards">${nodes.map(nodeCard).join("")}</div>` :
      `<div class="panel"><div class="empty">No E2 node connected. Point your gNB / simulator at this RIC's E2 endpoint.</div></div>`}`;
  }
  function nodeCard(n) {
    const kpm = n.ran_functions.find((f) => f.service_model === "KPM");
    const rc = n.ran_functions.find((f) => f.service_model === "RC");
    const nMetrics = kpm && kpm.capabilities ? new Set(kpm.capabilities.report_styles.flatMap((s) => s.metrics.map((m) => m.name))).size : 0;
    const nCtrl = rc && rc.capabilities ? rc.capabilities.control_styles.reduce((a, s) => a + s.actions.length, 0) : 0;
    return `<div class="panel node-card" data-href="#/nodes/${encodeURIComponent(n.id)}">
      <div class="title"><span class="name"><span class="dot ${n.connected ? "on" : "off"}"></span>${esc(n.id)}</span>
        <span class="muted">${esc(n.transport || "")}</span></div>
      <div class="row"><span>${nodeLabel(n)}</span><span>${esc(n.peer || "")}</span></div>
      <div class="row"><span><b>${n.ue_count}</b> UEs</span><span><b>${fmtNum(n.stats.indications)}</b> indications</span><span>up ${dur(Date.now() / 1000 - n.connected_at)}</span></div>
      <div class="fns">
        ${n.ran_functions.map((f) => `${smBadge(f.service_model)}<span class="muted">#${f.id}</span>`).join(" ")}
      </div>
      <div class="row">${kpm ? `<span>KPM: ${kpm.capabilities ? kpm.capabilities.report_styles.length : 0} report styles, ${nMetrics} metrics</span>` : ""}
        ${rc ? `<span>RC: ${rc.capabilities ? rc.capabilities.control_styles.length : 0} control styles, ${nCtrl} actions</span>` : ""}</div>
    </div>`;
  }

  function renderNode(id) {
    const n = state.nodes.find((x) => x.id === id);
    if (!n) return `<a class="back" href="#/nodes">← E2 nodes</a><div class="panel"><div class="empty">Unknown node ${esc(id)}</div></div>`;
    const i = n.info || {};
    const ues = state.ues.filter((u) => u.node_id === n.id);
    const subs = state.subs.filter((s) => s.node_id === n.id);
    const metrics = Object.entries(n.metrics || {});
    return `<a class="back" href="#/nodes">← E2 nodes</a>
    <div class="page-head"><div><h1 class="mono">${esc(n.id)}</h1>
      <div class="sub">${n.connected ? '<span class="badge b-ok">connected</span>' : '<span class="badge b-err">disconnected</span>'} ${nodeLabel(n)}</div></div>
      <div class="btns">
        <button data-act="query" data-node="${esc(n.id)}" ${n.connected ? "" : "disabled"}>RIC Service Query</button>
        <button data-act="reset" data-node="${esc(n.id)}" ${n.connected ? "" : "disabled"}>E2 Reset</button>
        <button class="danger" data-act="remove" data-node="${esc(n.id)}" ${n.connected ? "" : "disabled"}>E2 Removal</button>
      </div></div>
    <div class="grid grid-3">
      <div class="panel"><div class="panel-head"><h2>Identity</h2></div><div class="panel-body"><dl class="kv">
        <dt>Node type</dt><dd>${esc(i.type)}</dd><dt>PLMN</dt><dd>${esc(i.plmn)}</dd>
        <dt>NB ID</dt><dd class="mono">0x${hex(i.nb_id)} (${esc(i.nb_id_len)} bits)</dd>
        ${i.du_id != null ? `<dt>DU ID</dt><dd>${esc(i.du_id)}</dd>` : ""}${i.cu_up_id != null ? `<dt>CU-UP ID</dt><dd>${esc(i.cu_up_id)}</dd>` : ""}
        <dt>Peer</dt><dd class="mono">${esc(n.peer || "–")} (${esc(n.transport || "–")})</dd>
        <dt>Connected</dt><dd>${time(n.connected_at)} (${dur(Date.now() / 1000 - n.connected_at)})</dd>
        <dt>Last message</dt><dd>${ago(n.last_seen)}</dd>
      </dl></div></div>
      <div class="panel"><div class="panel-head"><h2>E2AP statistics</h2></div><div class="panel-body"><dl class="kv">
        <dt>Received</dt><dd>${fmtNum(n.stats.rx)}</dd><dt>Sent</dt><dd>${fmtNum(n.stats.tx)}</dd>
        <dt>Indications</dt><dd>${fmtNum(n.stats.indications)}</dd><dt>Errors</dt><dd>${fmtNum(n.stats.errors)}</dd>
        <dt>Subscriptions</dt><dd>${subs.length}</dd><dt>UEs</dt><dd>${ues.length}</dd></dl></div></div>
      <div class="panel"><div class="panel-head"><h2>E2 node components</h2></div><div class="panel-body flush">
        ${n.components.length ? `<table><thead><tr><th>Interface</th><th>Component ID</th></tr></thead><tbody>
          ${n.components.map((c) => `<tr><td>${esc((c.interface || "").toUpperCase())}</td><td class="mono">${esc(JSON.stringify(c.id))}</td></tr>`).join("")}</tbody></table>`
          : '<div class="empty">none</div>'}</div></div>
    </div>
    <div class="panel" style="margin-top:16px"><div class="panel-head"><h2>RAN functions &amp; service model capabilities</h2>
      <span class="muted">${n.ran_functions.length} function(s)</span></div>
      <div class="panel-body">${n.ran_functions.map(ranFunction).join("") || '<div class="empty">No RAN functions</div>'}</div></div>
    <div class="grid grid-2" style="margin-top:16px">
      <div class="panel"><div class="panel-head"><h2>Node KPIs (KPM)</h2>
        <div class="btns" style="align-items:center"><span class="muted">${n.metrics_ts ? "updated " + ago(n.metrics_ts) : "no node-level reports"}</span>
        ${exportButton("kpicsv", "data-node", n.id, state.nodeSamples[n.id] || [])}</div></div>
        <div class="panel-body flush">${metrics.length ? `<table><thead><tr><th>Metric</th><th class="num">Value</th><th>Trend</th></tr></thead><tbody>
          ${metrics.map(([k, v]) => `<tr><td class="mono">${esc(k)}</td><td class="num">${fmtNum(v, 2)}</td><td>${spark(`n:${n.id}:${k}`)}</td></tr>`).join("")}
          </tbody></table>` : '<div class="empty">No data</div>'}</div></div>
      <div class="panel"><div class="panel-head"><h2>UEs</h2></div><div class="panel-body flush table-wrap">${uesTable(ues, true)}</div></div>
    </div>
    <div class="panel" style="margin-top:16px"><div class="panel-head"><h2>Subscriptions</h2></div>
      <div class="panel-body flush table-wrap">${subsTable(subs)}</div></div>`;
  }

  function ranFunction(f) {
    const c = f.capabilities;
    const name = c && c.name ? c.name : {};
    let body;
    if (!c) body = `<div class="empty">Service model not recognised${f.error ? ": " + esc(f.error) : ""} (definition ${f.definition_len} bytes)</div>`;
    else if (f.service_model === "KPM") body = kpmCaps(c, f.id);
    else body = rcCaps(c, f.id);
    return `<div class="fn"><div class="fn-head">${smBadge(f.service_model)}
      <span class="t">RAN function ${f.id}${name.short_name ? " · " + esc(name.short_name) : ""}</span>
      <span class="muted">${esc(name.description || "")}</span>
      <span class="o">OID ${esc(f.oid)} · rev ${esc(f.revision)}${name.instance != null ? " · instance " + esc(name.instance) : ""}</span></div>
      <div class="fn-body">${body}</div></div>`;
  }

  function stylesTable(list, extraCols) {
    if (!list || !list.length) return '<div class="muted">none</div>';
    return `<table><thead><tr><th>Style</th><th>Name</th>${extraCols.map((c) => `<th>${c[0]}</th>`).join("")}</tr></thead><tbody>
      ${list.map((s) => `<tr><td>${esc(s.type)}</td><td>${esc(s.name)}</td>${extraCols.map((c) => `<td>${c[1](s)}</td>`).join("")}</tr>`).join("")}
      </tbody></table>`;
  }

  function kpmCaps(c, fid) {
    return `<h3>Event trigger styles</h3>${stylesTable(c.event_trigger_styles, [["Format", (s) => esc(s.format)]])}
      <h3>Report styles</h3>
      ${(c.report_styles || []).map((s) => `<div class="style-block"><div class="sh"><b>Style ${esc(s.type)}</b> ${esc(s.name)}
          <span class="fmt">action fmt ${esc(s.action_format)} · header fmt ${esc(s.header_format)} · message fmt ${esc(s.message_format)}</span>
          <span class="badge b-muted">${s.metrics.length} metrics</span></div>
        <div class="sb"><div class="chips">${s.metrics.map((m) => `<span class="chip" title="measID ${esc(m.id)}">${esc(m.name)}</span>`).join("")}</div></div></div>`).join("")
      || '<div class="muted">none</div>'}`;
  }

  function paramTree(params, key) {
    if (!params || !params.length) return "";
    return `<ul class="tree${key ? "" : " root"}">${params.map((p) => {
      const d = p.definition;
      return `<li><span class="pid">${esc(p.id)}</span>${esc(p.name)}${d ? ` <span class="badge b-muted">${esc(d.kind)}</span>${paramTree(d.params, true)}` : ""}</li>`;
    }).join("")}</ul>`;
  }

  function rcCaps(c, fid) {
    const ctl = (c.control_styles || []).map((s) => `<div class="style-block"><div class="sh"><b>Style ${esc(s.type)}</b> ${esc(s.name)}
        <span class="fmt">header fmt ${esc(s.header_format)} · message fmt ${esc(s.message_format)} · outcome fmt ${esc(s.outcome_format)}</span>
        <span class="badge b-muted">${s.actions.length} actions</span></div>
      <div class="sb">${s.actions.map((a) => `<details class="action" data-k="rc${fid}-c${s.type}-${a.id}"><summary>Action ${esc(a.id)}: ${esc(a.name)} <span class="muted">(${a.params.length} params)</span></summary>
          ${paramTree(a.params) || '<div class="muted">no parameters</div>'}</details>`).join("") || '<div class="muted">no actions</div>'}
        ${s.outcome_params && s.outcome_params.length ? `<details data-k="rc${fid}-co${s.type}"><summary>Outcome parameters</summary>${paramTree(s.outcome_params)}</details>` : ""}
      </div></div>`).join("");
    const rep = (c.report_styles || []).map((s) => `<div class="style-block"><div class="sh"><b>Style ${esc(s.type)}</b> ${esc(s.name)}
        <span class="fmt">event trigger style ${esc(s.event_trigger_style)} · action fmt ${esc(s.action_format)} · header fmt ${esc(s.header_format)} · message fmt ${esc(s.message_format)}</span></div>
      <div class="sb">${paramTree(s.params) || '<span class="muted">no parameters</span>'}</div></div>`).join("");
    const ins = (c.insert_styles || []).map((s) => `<div class="style-block"><div class="sh"><b>Style ${esc(s.type)}</b> ${esc(s.name)}</div>
      <div class="sb">${s.indications.map((i) => `<details data-k="rc${fid}-i${s.type}-${i.id}"><summary>Indication ${esc(i.id)}: ${esc(i.name)}</summary>${paramTree(i.params)}</details>`).join("")}</div></div>`).join("");
    const pol = (c.policy_styles || []).map((s) => `<div class="style-block"><div class="sh"><b>Style ${esc(s.type)}</b> ${esc(s.name)}</div>
      <div class="sb">${s.actions.map((a) => `<details data-k="rc${fid}-p${s.type}-${a.id}"><summary>Action ${esc(a.id)}: ${esc(a.name)}</summary>${paramTree(a.params)}
        ${a.condition_params && a.condition_params.length ? "<h3>Conditions</h3>" + paramTree(a.condition_params) : ""}</details>`).join("")}</div></div>`).join("");
    return `<h3>Event trigger styles</h3>${stylesTable(c.event_trigger_styles, [["Format", (s) => esc(s.format)]])}
      ${c.ue_identification_parameters && c.ue_identification_parameters.length ? `<h3>UE identification parameters</h3>${paramTree(c.ue_identification_parameters)}` : ""}
      ${c.call_process_types && c.call_process_types.length ? `<h3>Call process types</h3>${c.call_process_types.map((t) => `<div>${esc(t.id)}: ${esc(t.name)} – ${t.breakpoints.map((b) => esc(b.name)).join(", ")}</div>`).join("")}` : ""}
      <h3>Control styles</h3>${ctl || '<div class="muted">none</div>'}
      <h3>Report styles</h3>${rep || '<div class="muted">none</div>'}
      <h3>Insert styles</h3>${ins || '<div class="muted">none</div>'}
      <h3>Policy styles</h3>${pol || '<div class="muted">none</div>'}`;
  }

  const UE_COLS = [["RSRP", "RSRP"], ["CQI", "CQI"], ["DRB.UEThpDl", "DL kbps"], ["DRB.UEThpUl", "UL kbps"], ["RRU.PrbUsedDl", "PRB DL"]];
  function uesTable(ues, compact) {
    if (!ues.length) return `<div class="empty">No UEs</div>`;
    return `<table><thead><tr><th>UE</th>${compact ? "" : "<th>E2 node</th>"}<th>Serving cell</th>
      ${UE_COLS.map((c) => `<th class="num">${c[1]}</th>`).join("")}<th>Last seen</th></tr></thead><tbody>
      ${ues.map((u) => `<tr class="clickable" data-href="#/ues/${encodeURIComponent(u.key)}"><td class="mono">${esc(u.key)}</td>
        ${compact ? "" : `<td class="mono"><a href="#/nodes/${encodeURIComponent(u.node_id)}">${esc(u.node_id)}</a></td>`}
        <td class="mono">${cellStr(u.cell)}</td>
        ${UE_COLS.map((c) => `<td class="num">${fmtNum(u.metrics[c[0]])}</td>`).join("")}
        <td>${ago(u.last_seen)}</td></tr>`).join("")}</tbody></table>`;
  }

  function renderUEs() {
    const f = state.ueFilter.toLowerCase();
    const ues = state.ues.filter((u) => (!state.ueNode || u.node_id === state.ueNode) &&
      (!f || u.key.toLowerCase().includes(f) || JSON.stringify(u.ue_id).toLowerCase().includes(f)));
    return `<div class="page-head"><div><h1>UEs</h1><div class="sub">UEs learned from E2SM-KPM and E2SM-RC reports</div></div>
      <div class="filters"><input id="ue-filter" placeholder="filter UEs…" value="${esc(state.ueFilter)}">
      <select id="ue-node"><option value="">all nodes</option>${state.nodes.map((n) => `<option ${n.id === state.ueNode ? "selected" : ""}>${esc(n.id)}</option>`).join("")}</select></div></div>
      <div class="panel table-wrap">${uesTable(ues)}</div>`;
  }

  function renderUE(key) {
    const u = state.ues.find((x) => x.key === key);
    if (!u) return `<a class="back" href="#/ues">← UEs</a><div class="panel"><div class="empty">UE ${esc(key)} is no longer known</div></div>`;
    const metrics = Object.entries(u.metrics || {});
    return `<a class="back" href="#/ues">← UEs</a>
      <div class="page-head"><div><h1 class="mono">${esc(u.key)}</h1><div class="sub">served by <a class="mono" href="#/nodes/${encodeURIComponent(u.node_id)}">${esc(u.node_id)}</a> · cell ${cellStr(u.cell)}</div></div></div>
      <div class="grid grid-2">
        <div class="panel"><div class="panel-head"><h2>Identity</h2></div><div class="panel-body"><dl class="kv">
          ${Object.entries(u.ue_id).map(([k, v]) => `<dt>${esc(k)}</dt><dd class="mono">${esc(typeof v === "object" ? JSON.stringify(v) : v)}</dd>`).join("")}
          <dt>First seen</dt><dd>${time(u.first_seen)}</dd><dt>Last seen</dt><dd>${ago(u.last_seen)}</dd>
          ${u.attrs.previous_node ? `<dt>Previous node</dt><dd class="mono">${esc(u.attrs.previous_node)}</dd>` : ""}</dl></div></div>
        <div class="panel"><div class="panel-head"><h2>KPM metrics</h2>
          <div class="btns" style="align-items:center"><span class="muted">${u.metrics_ts ? "updated " + ago(u.metrics_ts) : ""}</span>
          ${exportButton("uekpicsv", "data-ue", u.key, state.ueSamples[u.key] || [])}</div></div>
          <div class="panel-body flush">${metrics.length ? `<table><thead><tr><th>Metric</th><th class="num">Value</th><th>Trend</th></tr></thead><tbody>
          ${metrics.map(([k, v]) => `<tr><td class="mono">${esc(k)}</td><td class="num">${fmtNum(v, 2)}</td><td>${spark(`u:${u.key}:${k}`)}</td></tr>`).join("")}</tbody></table>`
          : '<div class="empty">No UE level KPM reports</div>'}</div></div>
      </div>
      ${Object.keys(u.rc_params || {}).length ? `<div class="panel" style="margin-top:16px"><div class="panel-head"><h2>RC reported RAN parameters</h2></div>
        <div class="panel-body"><pre class="json">${esc(JSON.stringify(u.rc_params, null, 2))}</pre></div></div>` : ""}`;
  }

  function subsTable(subs) {
    if (!subs.length) return `<div class="empty">No subscriptions</div>`;
    return `<table><thead><tr><th>Subscription</th><th>RAN function</th><th>Owner</th><th>Details</th><th>State</th>
      <th class="num">Indications</th><th>Last</th><th></th></tr></thead><tbody>
      ${subs.map((s) => {
        const d = s.description || {};
        const det = [d.style != null ? "style " + d.style : "", d.period_ms ? d.period_ms + " ms" : "",
          d.metrics ? d.metrics.length + " metrics" : ""].filter(Boolean).join(" · ");
        return `<tr><td class="mono">${esc(s.id)}</td><td>${smBadge(s.service_model)} #${esc(s.ran_function_id)}</td>
          <td>${esc(s.owner)}</td><td class="muted" title="${esc((d.metrics || []).join(", "))}">${esc(det)}</td>
          <td>${stateBadge(s.state)}</td><td class="num">${fmtNum(s.indications)}</td><td>${ago(s.last_indication)}</td>
          <td class="right"><button class="danger" data-act="unsub" data-id="${esc(s.id)}">Delete</button></td></tr>`;
      }).join("")}</tbody></table>`;
  }
  function renderSubs() {
    return `<div class="page-head"><div><h1>Subscriptions</h1><div class="sub">Active RIC subscriptions on all E2 nodes</div></div></div>
      <div class="panel table-wrap">${subsTable(state.subs)}</div>`;
  }

  function renderXApps() {
    return `<div class="page-head"><div><h1>xApps</h1><div class="sub">Embedded xApps (installed packages with a <code>uric.xapps</code> entry point, or given with <code>--xapp</code> / in the configuration file) and standalone xApps connected to the xApp interface</div></div></div>
      <div class="grid grid-2">${state.xapps.map((x) => `<div class="panel">
        <div class="panel-head"><h2>${esc(x.name)}${x.version ? ` <span class="muted">v${esc(x.version)}</span>` : ""}</h2>${stateBadge(x.state)}</div>
        <div class="panel-body">
          <dl class="kv">
          ${x.version ? `<dt>Version</dt><dd>${esc(x.version)}</dd>` : ""}
          ${x.description ? `<dt>Description</dt><dd class="prose">${esc(x.description)}</dd>` : ""}
          <dt>Source</dt><dd>${esc(x.source)}</dd><dt>Spec</dt><dd class="mono">${esc(x.spec)}</dd>
          ${x.requestor_id != null ? `<dt>RIC requestor ID</dt><dd>${esc(x.requestor_id)}</dd>` : ""}
          ${x.started_at ? `<dt>Started</dt><dd>${time(x.started_at)}</dd>` : ""}
          <dt>Subscriptions</dt><dd>${x.subscriptions.length}</dd>
          ${x.error ? `<dt>Error</dt><dd style="color:var(--err)">${esc(x.error)}</dd>` : ""}</dl>
          ${Object.keys(x.config || {}).length ? `<details data-k="xc-${esc(x.name)}"><summary>Configuration</summary><pre class="json">${esc(JSON.stringify(x.config, null, 2))}</pre></details>` : ""}
          ${x.source !== "remote" && Object.keys(x.start_args || {}).length ? `<details data-k="xa-${esc(x.name)}"><summary>Start arguments</summary><pre class="json">${esc(JSON.stringify(x.start_args, null, 2))}</pre></details>` : ""}
          ${Object.keys(x.status || {}).length ? `<details data-k="xs-${esc(x.name)}" open><summary>Status ${x.status_ts ? `<span class="muted">(${ago(x.status_ts)})</span>` : ""}</summary><div class="copy-wrap">
            <button type="button" class="copy-btn" data-act="xcopy" data-name="${esc(x.name)}" title="Copy status to clipboard" aria-label="Copy the status of ${esc(x.name)} to the clipboard">
              <svg viewBox="0 0 16 16" aria-hidden="true"><rect x="5.5" y="5.5" width="8" height="9" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M10.5 3.5V2.9A1.4 1.4 0 0 0 9.1 1.5H3.9A1.4 1.4 0 0 0 2.5 2.9v7.2a1.4 1.4 0 0 0 1.4 1.4h.6" fill="none" stroke="currentColor" stroke-width="1.4"/></svg>Copy</button>
            <pre class="json">${esc(JSON.stringify(x.status, null, 2))}</pre></div></details>` : ""}
          <div class="btns" style="margin-top:10px">
            ${x.source === "remote" ? (x.state === "running" ? `<button data-act="xstop" data-name="${esc(x.name)}">Disconnect</button>` : "")
              : x.state === "running" ? `<button data-act="xstop" data-name="${esc(x.name)}">Stop</button>`
              : `<button class="primary" data-act="xstart" data-name="${esc(x.name)}">Start</button>`}</div>
        </div></div>`).join("")}
      </div>`;
  }

  function eventRows(evs) {
    if (!evs.length) return `<div class="empty">No events yet</div>`;
    return evs.map((e) => `<div class="ev"><span class="ts">${time(e.ts)}</span><span>${evBadge(e.event)}</span>
      <span class="data">${esc(evSummary(e))}</span></div>`).join("");
  }
  function evBadge(e) {
    const cls = e.startsWith("node.dis") || e === "error" || e.endsWith("failed") ? "b-err"
      : e.startsWith("node") ? "b-ok" : e.startsWith("ue") ? "b-kpm" : e.startsWith("control") ? "b-rc" : "b-accent";
    return `<span class="badge ${cls}">${esc(e)}</span>`;
  }
  function evSummary(e) {
    const d = e.data || {};
    switch (e.event) {
      case "node.connected": case "node.updated": return `${d.id} (${(d.ran_functions || []).map((f) => f.id + ":" + f.service_model).join(", ")})`;
      case "node.disconnected": return d.id;
      case "ue.added": return `${d.key} on ${d.node_id}`;
      case "ue.removed": return `${d.key} from ${d.node_id} (${d.reason})`;
      case "subscription.created": case "subscription.deleted": case "subscription.failed":
        return `${d.id} owner=${d.owner} sm=${d.service_model}${d.error ? " error=" + d.error : ""}`;
      case "control": return `${d.owner} -> ${d.node_id}: ${d.success ? "ack" : "failure " + (d.cause || "")}`;
      case "xapp": return `${d.name}: ${d.state}${d.error ? " (" + d.error + ")" : ""}`;
      default: return JSON.stringify(d);
    }
  }
  function renderEvents() {
    const f = state.evFilter.toLowerCase();
    const evs = state.events.filter((e) => !f || e.event.includes(f) || evSummary(e).toLowerCase().includes(f));
    return `<div class="page-head"><div><h1>Events</h1><div class="sub">Live platform event stream</div></div>
      <div class="filters"><input id="ev-filter" placeholder="filter…" value="${esc(state.evFilter)}"></div></div>
      <div class="panel events">${eventRows(evs)}</div>`;
  }

  // ------------------------------------------------------------ actions
  function bind() {
    view.querySelectorAll("[data-href]").forEach((el) => el.addEventListener("click", (ev) => {
      if (ev.target.closest("a,button")) return;
      location.hash = el.dataset.href;
    }));
    const uf = document.getElementById("ue-filter");
    if (uf) uf.addEventListener("input", () => { state.ueFilter = uf.value; render(true); const e = document.getElementById("ue-filter"); e.focus(); e.setSelectionRange(e.value.length, e.value.length); });
    const un = document.getElementById("ue-node");
    if (un) un.addEventListener("change", () => { state.ueNode = un.value; render(true); });
    const ef = document.getElementById("ev-filter");
    if (ef) ef.addEventListener("input", () => { state.evFilter = ef.value; render(true); const e = document.getElementById("ev-filter"); e.focus(); e.setSelectionRange(e.value.length, e.value.length); });
    view.querySelectorAll("button[data-act]").forEach((b) => b.addEventListener("click", () => act(b)));
  }

  async function copyText(text) {
    // The Clipboard API needs a secure context (https or localhost) and may be refused by the
    // browser (permissions, embedded webviews): fall back to execCommand("copy") in both cases.
    if (navigator.clipboard && window.isSecureContext) {
      try {
        await navigator.clipboard.writeText(text);
        return;
      } catch (e) { /* fall back below */ }
    }
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try {
      if (!document.execCommand("copy")) throw new Error("the browser refused to copy");
    } finally {
      document.body.removeChild(ta);
    }
  }

  async function act(b) {
    const a = b.dataset.act;
    if (a === "kpicsv") {
      const node = b.dataset.node;
      const samples = state.nodeSamples[node] || [];
      if (!samples.length) { toast("No node KPIs to export yet", true); return; }
      downloadFile(`${node}-kpis-${fileStamp()}.csv`, kpisCsv(samples, { node_id: node }), "text/csv;charset=utf-8");
      toast(`Exported ${samples.length} KPM report(s) of ${node}`);
      return;
    }
    if (a === "uekpicsv") {
      const ue = b.dataset.ue;
      const samples = state.ueSamples[ue] || [];
      if (!samples.length) { toast("No UE KPM metrics to export yet", true); return; }
      // UE keys contain ":" (e.g. "gNB-UEID:7"), which is not allowed in file names on every OS
      const fname = `${ue.replace(/[^A-Za-z0-9._-]+/g, "_")}-kpis-${fileStamp()}.csv`;
      downloadFile(fname, kpisCsv(samples, { ue_key: ue, node_id: (smp) => smp.extra.node_id }),
        "text/csv;charset=utf-8");
      toast(`Exported ${samples.length} KPM report(s) of UE ${ue}`);
      return;
    }
    if (a === "xcopy") {
      const x = state.xapps.find((y) => y.name === b.dataset.name);
      try {
        await copyText(JSON.stringify((x && x.status) || {}, null, 2));
        toast(`Status of ${b.dataset.name} copied to the clipboard`);
      } catch (e) {
        toast(`Copy failed: ${e.message}`, true);
      }
      return;
    }
    b.disabled = true;
    try {
      if (a === "unsub") {
        const [node, req] = splitSub(b.dataset.id);
        await api(`subscriptions/${encodeURIComponent(node)}/${req}`, { method: "DELETE" });
        toast("Subscription deleted");
      } else if (a === "xstart") {
        const r = await api(`xapps/${encodeURIComponent(b.dataset.name)}/start`, { method: "POST", body: "{}" });
        toast(r.state === "running" ? `${r.name} started` : `${r.name}: ${r.error}`, r.state !== "running");
      } else if (a === "xstop") {
        await api(`xapps/${encodeURIComponent(b.dataset.name)}/stop`, { method: "POST" });
        toast(`${b.dataset.name} stopped`);
      } else if (a === "query" || a === "reset" || a === "remove") {
        if (a !== "query" && !confirm(`${a === "reset" ? "Reset" : "Remove"} E2 node ${b.dataset.node}?`)) return;
        const r = await api(`nodes/${encodeURIComponent(b.dataset.node)}/${a}`, { method: "POST" });
        toast(a === "query" ? `Service query answered with ${r.response}` : `E2 ${a} done`);
      }
    } catch (e) {
      toast(e.message, true);
    } finally {
      b.disabled = false;
      scheduleRefresh();
    }
  }
  function splitSub(id) {
    const i = id.lastIndexOf("/");
    return [id.slice(0, i), id.slice(i + 1)];
  }

  // ------------------------------------------------------------ theme (auto / light / dark)
  const THEME_KEY = "uric.theme";
  function savedTheme() {
    try {
      const t = localStorage.getItem(THEME_KEY);
      return t === "light" || t === "dark" ? t : "auto";
    } catch (e) { return "auto"; }
  }
  function applyTheme(choice) {
    if (choice === "light" || choice === "dark") document.documentElement.setAttribute("data-theme", choice);
    else document.documentElement.removeAttribute("data-theme");   // auto: follow prefers-color-scheme
    document.querySelectorAll("#theme-toggle [data-theme-choice]").forEach((b) =>
      b.setAttribute("aria-pressed", String(b.dataset.themeChoice === choice)));
  }
  function initTheme() {
    applyTheme(savedTheme());
    document.querySelectorAll("#theme-toggle [data-theme-choice]").forEach((b) => b.addEventListener("click", () => {
      const choice = b.dataset.themeChoice;
      try {
        if (choice === "auto") localStorage.removeItem(THEME_KEY); else localStorage.setItem(THEME_KEY, choice);
      } catch (e) { /* not persisted, still applied for this page */ }
      applyTheme(choice);
    }));
  }

  // ------------------------------------------------------------ boot
  async function boot() {
    initTheme();
    try { state.events = (await api("events/history?limit=300")).reverse(); } catch (e) { /* ignore */ }
    await refresh();
    connectEvents();
    setInterval(refresh, 2000);
  }
  boot();
})();
