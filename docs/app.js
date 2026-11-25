const state = {
  payload: null,
  games: [],
  search: "",
  filter: "all",
  minEdge: 0,
  sortKey: "abs_edge",
  sortDir: "desc",
};

const els = {
  dateSelect: document.getElementById("date-select"),
  updatedText: document.getElementById("updated-text"),
  statTotal: document.getElementById("stat-total"),
  statOdds: document.getElementById("stat-odds"),
  statTopEdge: document.getElementById("stat-top-edge"),
  statTopMatchup: document.getElementById("stat-top-matchup"),
  searchInput: document.getElementById("team-search"),
  pillBtns: document.querySelectorAll(".pill-btn"),
  edgeSlider: document.getElementById("min-edge-slider"),
  edgeSliderVal: document.getElementById("min-edge-val"),
  tbody: document.getElementById("predictions-body"),
  headers: document.querySelectorAll("thead th.sortable"),
};

function fmtSigned(val) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return "—";
  const num = Number(val);
  return num > 0 ? `+${num.toFixed(2)}` : num.toFixed(2);
}

function fmtSpread(val) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return "—";
  const num = Number(val);
  return num > 0 ? `+${num.toFixed(1)}` : num.toFixed(1);
}

function getModelLean(g) {
  if (g.spread === null || g.spread === undefined || g.difference === null || g.difference === undefined) {
    const fav = g.prediction >= 0 ? g.home_name : g.away_name;
    return { text: `${fav} by ${Math.abs(g.prediction).toFixed(1)}`, cls: "muted" };
  }
  const diff = Number(g.difference);
  if (Math.abs(diff) < 0.05) return { text: "No Edge", cls: "muted" };
  if (diff > 0) {
    const homeLine = -Number(g.spread);
    return { text: `${g.home_name} (${homeLine > 0 ? "+" : ""}${homeLine.toFixed(1)})`, cls: "lean-home" };
  }
  const awayLine = Number(g.spread);
  return { text: `${g.away_name} (${awayLine > 0 ? "+" : ""}${awayLine.toFixed(1)})`, cls: "lean-away" };
}

function updateSummary(payload) {
  const games = payload.games || [];
  const withOdds = games.filter((g) => g.spread !== null && g.spread !== undefined);
  els.statTotal.textContent = games.length;
  els.statOdds.textContent = withOdds.length;
  els.updatedText.textContent = payload.updated_at ? `Updated ${payload.updated_at}` : "";

  if (withOdds.length > 0) {
    const top = [...withOdds].sort((a, b) => Math.abs(b.difference) - Math.abs(a.difference))[0];
    const lean = getModelLean(top);
    els.statTopEdge.textContent = `${Math.abs(top.difference).toFixed(2)} pts`;
    els.statTopMatchup.textContent = `${lean.text} (${top.away_name} @ ${top.home_name})`;
  } else {
    els.statTopEdge.textContent = "—";
    els.statTopMatchup.textContent = "No Vegas lines posted yet";
  }
}

function getFilteredGames() {
  const q = state.search.trim().toLowerCase();
  return state.games
    .filter((g) => {
      if (q && !g.away_name.toLowerCase().includes(q) && !g.home_name.toLowerCase().includes(q)) return false;
      const hasOdds = g.spread !== null && g.spread !== undefined;
      if (state.filter === "odds" && !hasOdds) return false;
      if (state.filter === "neutral" && Number(g.neutral) !== 1) return false;
      if (state.filter === "high-edge" && (!hasOdds || Math.abs(g.difference) < 2.0)) return false;
      if (state.minEdge > 0 && (!hasOdds || Math.abs(g.difference) < state.minEdge)) return false;
      return true;
    })
    .sort((a, b) => {
      const dir = state.sortDir === "asc" ? 1 : -1;
      const key = state.sortKey;
      if (key === "away_name" || key === "home_name") return a[key].localeCompare(b[key]) * dir;
      const getNum = (row) => {
        if (key === "abs_edge") return row.difference === null || row.difference === undefined ? -1 : Math.abs(Number(row.difference));
        const v = row[key];
        return v === null || v === undefined ? -9999 : Number(v);
      };
      return (getNum(a) - getNum(b)) * dir;
    });
}

function renderTable() {
  const rows = getFilteredGames();
  if (!rows.length) {
    els.tbody.innerHTML = `<tr><td colspan="6" class="empty-state">No games match the current filters.</td></tr>`;
    return;
  }
  els.tbody.innerHTML = rows
    .map((g) => {
      const hasOdds = g.spread !== null && g.spread !== undefined;
      const absEdge = hasOdds ? Math.abs(Number(g.difference)) : 0;
      const edgeClass = hasOdds && absEdge >= 2.0 ? "edge-strong" : "edge-mild";
      const lean = getModelLean(g);
      const isNeutral = Number(g.neutral) === 1;
      return `
        <tr>
          <td>
            <div class="matchup-cell">
              <span class="matchup-teams">${g.away_name} ${isNeutral ? "vs." : "@"} ${g.home_name}</span>
              <span class="matchup-sub">${g.date || ""}</span>
            </div>
          </td>
          <td><span class="site-badge ${isNeutral ? "neutral" : ""}">${isNeutral ? "Neutral" : "Home"}</span></td>
          <td class="mono">${fmtSpread(g.spread)}</td>
          <td class="mono">${fmtSigned(g.prediction)}</td>
          <td>${hasOdds ? `<span class="edge-badge mono ${edgeClass}">${fmtSigned(g.difference)}</span>` : `<span class="muted mono">—</span>`}</td>
          <td><span class="lean-text ${lean.cls}">${lean.text}</span></td>
        </tr>`;
    })
    .join("");
}

function updateSortHeaders() {
  els.headers.forEach((th) => {
    th.classList.remove("sorted-asc", "sorted-desc");
    if (th.dataset.sort === state.sortKey) th.classList.add(state.sortDir === "asc" ? "sorted-asc" : "sorted-desc");
  });
}

async function loadPredictions(file = "data/latest.json") {
  try {
    const resp = await fetch(`${file}?t=${Date.now()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const payload = await resp.json();
    state.payload = payload;
    state.games = payload.games || [];
    updateSummary(payload);
    updateSortHeaders();
    renderTable();
  } catch (err) {
    els.tbody.innerHTML = `<tr><td colspan="6" class="empty-state">Unable to load predictions (${err.message}).</td></tr>`;
  }
}

async function loadAvailableDates() {
  try {
    const resp = await fetch(`data/dates.json?t=${Date.now()}`);
    if (!resp.ok) return;
    const dates = await resp.json();
    if (!Array.isArray(dates) || !dates.length) return;
    els.dateSelect.innerHTML = dates
      .map((d, idx) => `<option value="data/${d}.json">${idx === 0 ? `${d} (Latest)` : d}</option>`)
      .join("");
  } catch {}
}

els.searchInput.addEventListener("input", (e) => { state.search = e.target.value; renderTable(); });
els.pillBtns.forEach((btn) => {
  btn.addEventListener("click", () => {
    els.pillBtns.forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    state.filter = btn.dataset.filter;
    renderTable();
  });
});
els.edgeSlider.addEventListener("input", (e) => {
  state.minEdge = Number(e.target.value);
  els.edgeSliderVal.textContent = `${state.minEdge.toFixed(1)} pts`;
  renderTable();
});
els.headers.forEach((th) => {
  th.addEventListener("click", () => {
    const key = th.dataset.sort;
    if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    else { state.sortKey = key; state.sortDir = key === "away_name" ? "asc" : "desc"; }
    updateSortHeaders();
    renderTable();
  });
});
els.dateSelect.addEventListener("change", (e) => loadPredictions(e.target.value));
loadAvailableDates().then(() => loadPredictions("data/latest.json"));
