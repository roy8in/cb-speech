const PIPELINE_COLUMNS = [
  { id: "prepare", label: "Prepare" },
  { id: "collect", label: "Collect" },
  { id: "maintenance", label: "Maintenance" },
  { id: "analyze", label: "Analyze" },
  { id: "sync", label: "Sync" },
  { id: "finish", label: "Finish" },
];

const JOB_SPECS = [
  { key: "prepare_environment", column: "prepare", label: "Prepare environment" },
  { key: "collection", column: "collect", label: "Collection summary" },
  { key: "collect_frb", column: "collect", label: "FRB" },
  { key: "collect_ecb", column: "collect", label: "ECB" },
  { key: "collect_boe", column: "collect", label: "BOE" },
  { key: "collect_boj", column: "collect", label: "BOJ" },
  { key: "collect_rba", column: "collect", label: "RBA" },
  { key: "collect_boc", column: "collect", label: "BOC" },
  { key: "member_cleanup", column: "maintenance", label: "Member cleanup" },
  { key: "initial_analysis", column: "analyze", label: "Initial analysis" },
  { key: "exhaustive_analysis", column: "analyze", label: "Exhaustive analysis" },
  { key: "collection_sync", column: "sync", label: "Collection sync" },
  { key: "postgres_sync", column: "sync", label: "PostgreSQL sync" },
];

const SUCCESS_TONES = ["#0b7f44", "#15965a", "#2e7d32", "#23815f", "#4d8f3a", "#007a68"];

const els = {
  date: document.querySelector("#logDate"),
  notice: document.querySelector("#notice"),
  status: document.querySelector("#statusText"),
  runId: document.querySelector("#runIdText"),
  started: document.querySelector("#startedText"),
  ended: document.querySelector("#endedText"),
  duration: document.querySelector("#durationText"),
  rows: document.querySelector("#rowsText"),
  eventCount: document.querySelector("#eventCount"),
  durationBars: document.querySelector("#durationBars"),
  pipeline: document.querySelector("#pipeline"),
  openIssues: document.querySelector("#openIssues"),
  closeIssues: document.querySelector("#closeIssues"),
  issueDrawer: document.querySelector("#issueDrawer"),
  issueCount: document.querySelector("#issueCount"),
  issues: document.querySelector("#issues"),
  levelFilter: document.querySelector("#levelFilter"),
  search: document.querySelector("#searchBox"),
  eventRows: document.querySelector("#eventRows"),
};

let currentEvents = [];
let currentJobs = [];
let issueScope = null;

function todayIso() {
  const now = new Date();
  const tzOffset = now.getTimezoneOffset() * 60000;
  return new Date(now.getTime() - tzOffset).toISOString().slice(0, 10);
}

function parseLine(line) {
  const parts = line.split(" | ");
  if (parts.length < 4) {
    return null;
  }

  const [timestamp, level, logger, ...messageParts] = parts;
  const rawMessage = messageParts.join(" | ");
  const [message, extraText = ""] = rawMessage.split(" | ", 2);
  return {
    timestamp,
    level: level.trim(),
    logger: logger.trim(),
    message: message.trim(),
    extraText: extraText.trim(),
    raw: line,
    extra: parseExtra(extraText),
  };
}

function parseExtra(extraText) {
  const result = {};
  if (!extraText) {
    return result;
  }

  for (const part of extraText.split(", ")) {
    const index = part.indexOf("=");
    if (index <= 0) {
      continue;
    }
    result[part.slice(0, index).trim()] = part.slice(index + 1).trim();
  }
  return result;
}

function parseLog(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map(parseLine)
    .filter(Boolean);
}

function selectLatestRun(events) {
  const startIndex = events.map((event, index) => ({ event, index }))
    .reverse()
    .find((item) => item.event.message === "Starting sync run")?.index;

  if (startIndex === undefined) {
    return events;
  }

  const endOffset = events.slice(startIndex).findIndex((event) => event.message === "Finished sync run");
  if (endOffset < 0) {
    return events.slice(startIndex);
  }
  return events.slice(startIndex, startIndex + endOffset + 1);
}

function parseTimestamp(value) {
  if (!value) {
    return null;
  }
  const normalized = value.replace(" ", "T").replace(",", ".");
  const time = Date.parse(normalized);
  return Number.isFinite(time) ? time : null;
}

function secondsBetween(start, end) {
  const startMs = parseTimestamp(start);
  const endMs = parseTimestamp(end);
  if (startMs === null || endMs === null || endMs < startMs) {
    return 0;
  }
  return (endMs - startMs) / 1000;
}

function timeOnly(timestamp) {
  if (!timestamp) {
    return "-";
  }
  const match = timestamp.match(/\d{2}:\d{2}:\d{2}/);
  return match ? match[0] : timestamp;
}

function fmtSeconds(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return "-";
  }
  if (parsed < 60) {
    return `${parsed.toFixed(1)}s`;
  }
  return `${Math.floor(parsed / 60)}m ${Math.round(parsed % 60)}s`;
}

function fmtNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString() : "-";
}

function findFirst(events, tokens) {
  return events.find((event) => tokens.some((token) => event.message.includes(token) || event.extraText.includes(token)));
}

function findLast(events, tokens) {
  return [...events].reverse().find((event) => tokens.some((token) => event.message.includes(token) || event.extraText.includes(token)));
}

function eventsBetween(events, start, end) {
  if (!start && !end) {
    return [];
  }
  const startMs = parseTimestamp(start?.timestamp) ?? -Infinity;
  const endMs = parseTimestamp(end?.timestamp) ?? Infinity;
  return events.filter((event) => {
    const eventMs = parseTimestamp(event.timestamp);
    return eventMs !== null && eventMs >= startMs && eventMs <= endMs;
  });
}

function makeJob({ key, column, label, start, end, detail = "", status, duration, events = [] }) {
  const jobEvents = events.length ? events : eventsBetween(currentEvents, start, end);
  const hasError = jobEvents.some((event) => ["ERROR", "EXCEPTION"].includes(event.level));
  const hasWarning = jobEvents.some((event) => event.level === "WARNING");
  const inferredStatus = status ?? (!start && !end ? "unknown" : hasError ? "failed" : hasWarning ? "warning" : end ? "success" : "running");
  return {
    key,
    column,
    label,
    startTime: start?.timestamp ?? end?.timestamp ?? "",
    endTime: end?.timestamp ?? start?.timestamp ?? "",
    duration: Number.isFinite(Number(duration)) ? Number(duration) : (start && end ? secondsBetween(start.timestamp, end.timestamp) : 0),
    status: normalizeStatus(inferredStatus),
    detail,
    events: jobEvents,
    issues: jobEvents.filter((event) => ["WARNING", "ERROR", "EXCEPTION"].includes(event.level)),
  };
}

function normalizeStatus(status) {
  const value = String(status || "unknown").toLowerCase();
  if (["success", "failed", "running", "skipped", "warning"].includes(value)) {
    return value;
  }
  if (value === "partial") {
    return "warning";
  }
  if (value === "fail" || value === "error") {
    return "failed";
  }
  return "unknown";
}

function buildPipelineJobs(events) {
  const jobs = JOB_SPECS
    .map((spec) => buildStructuredJob(events, spec))
    .filter(Boolean);

  const finish = findLast(events, ["Finished sync run", "Everything is up-to-date"]);
  jobs.push(makeJob({
    key: "finish",
    column: "finish",
    label: "Finish run",
    start: finish,
    end: finish,
    status: finish?.extra.status || inferStatus(events),
    duration: finish?.extra.duration_sec,
    detail: finish ? [
      `new ${fmtNumber(finish.extra.total_new)}`,
      `refreshed ${fmtNumber(finish.extra.total_refreshed)}`,
      `analyzed ${fmtNumber(finish.extra.analyzed_items)}`,
      `synced ${fmtNumber(finish.extra.synced_items)}`,
      finish.extra.tableau_mart_items ? `marts ${fmtNumber(finish.extra.tableau_mart_items)}` : "",
    ].filter(Boolean).join(" - ") : "",
    events: finish ? [finish] : [],
  }));

  return jobs;
}

function buildStructuredJob(events, spec) {
  const jobEvents = events.filter((event) => (
    event.message === "Pipeline job status" &&
    event.extra.job_name === spec.key
  ));
  if (!jobEvents.length) {
    return null;
  }

  const start = jobEvents.find((event) => event.extra.status === "running") ?? jobEvents[0];
  const end = [...jobEvents].reverse().find((event) => event.extra.status !== "running") ?? null;
  const statusEvent = end ?? start;
  const scopedEvents = eventsBetween(events, start, end ?? start);
  const detail = buildJobDetail(spec.key, statusEvent, events);

  return makeJob({
    key: spec.key,
    column: spec.column,
    label: spec.label,
    start,
    end: end ?? start,
    status: statusEvent?.extra.status,
    duration: statusEvent?.extra.duration_sec,
    detail,
    events: scopedEvents.length ? scopedEvents : jobEvents,
  });
}

function buildJobDetail(key, event, events) {
  if (!event) {
    return "";
  }
  if (key.startsWith("collect_")) {
    return [
      `new ${fmtNumber(event.extra.new_items)}`,
      `refreshed ${fmtNumber(event.extra.refreshed_items)}`,
      event.extra.error_message ? `error ${event.extra.error_message}` : "",
    ].filter(Boolean).join(" - ");
  }
  if (key === "collection") {
    return `new ${fmtNumber(event.extra.total_new)} - refreshed ${fmtNumber(event.extra.total_refreshed)}`;
  }
  if (key.includes("analysis")) {
    return `analyzed ${fmtNumber(event.extra.analyzed_items)}`;
  }
  if (key.includes("sync")) {
    return [
      `synced ${fmtNumber(event.extra.synced_items)}`,
      `source ${fmtNumber(event.extra.source_synced_items)}`,
      `marts ${fmtNumber(event.extra.tableau_mart_items)}`,
      event.extra.mart_events_rows ? `events ${fmtNumber(event.extra.mart_events_rows)}` : "",
      event.extra.mart_daily_rows ? `daily ${fmtNumber(event.extra.mart_daily_rows)}` : "",
      event.extra.mart_plot_rows ? `plot ${fmtNumber(event.extra.mart_plot_rows)}` : "",
    ].filter(Boolean).join(" - ");
  }
  if (key === "finish") {
    return [
      `new ${fmtNumber(event.extra.total_new)}`,
      `analyzed ${fmtNumber(event.extra.analyzed_items)}`,
      `synced ${fmtNumber(event.extra.synced_items)}`,
      event.extra.tableau_mart_items ? `marts ${fmtNumber(event.extra.tableau_mart_items)}` : "",
    ].filter(Boolean).join(" - ");
  }
  if (event.extra.reason) {
    return event.extra.reason;
  }
  if (event.extra.error_message) {
    return event.extra.error_message;
  }
  return "";
}

function inferStatus(events) {
  if (events.some((event) => ["ERROR", "EXCEPTION"].includes(event.level))) {
    return "failed";
  }
  if (events.some((event) => event.message === "Finished sync run")) {
    return "success";
  }
  return "unknown";
}

function getRunSummary(events) {
  const start = events.find((event) => event.message === "Starting sync run");
  const finish = [...events].reverse().find((event) => event.message === "Finished sync run");
  return {
    status: finish?.extra.status ?? inferStatus(events).toUpperCase(),
    runId: finish?.extra.run_id ?? start?.extra.run_id ?? "-",
    started: timeOnly(start?.timestamp ?? events[0]?.timestamp),
    ended: timeOnly(finish?.timestamp ?? events.at(-1)?.timestamp),
    duration: finish?.extra.duration_sec ? fmtSeconds(Number(finish.extra.duration_sec)) : fmtSeconds(secondsBetween(events[0]?.timestamp, events.at(-1)?.timestamp)),
    totalNew: fmtNumber(finish?.extra.total_new),
    totalRefreshed: fmtNumber(finish?.extra.total_refreshed),
    analyzedItems: fmtNumber(finish?.extra.analyzed_items),
    syncedItems: fmtNumber(finish?.extra.synced_items),
    sourceSyncedItems: fmtNumber(finish?.extra.source_synced_items),
    tableauMartItems: fmtNumber(finish?.extra.tableau_mart_items),
    failedSteps: finish?.extra.failed_steps || "-",
  };
}

function renderSummary(events) {
  const summary = getRunSummary(events);
  const statusClass = normalizeStatus(summary.status);
  els.status.className = `status-pill ${statusClass}`;
  els.status.textContent = summary.status;
  els.runId.textContent = summary.runId;
  els.started.textContent = summary.started;
  els.ended.textContent = summary.ended;
  els.duration.textContent = summary.duration;
  els.rows.innerHTML = `
    <div class="count-grid">
      <div class="count-item"><span>New</span><strong>${summary.totalNew}</strong></div>
      <div class="count-item"><span>Refreshed</span><strong>${summary.totalRefreshed}</strong></div>
      <div class="count-item"><span>Analyzed</span><strong>${summary.analyzedItems}</strong></div>
      <div class="count-item"><span>Synced</span><strong>${summary.syncedItems}</strong></div>
      <div class="count-item"><span>Source Sync</span><strong>${summary.sourceSyncedItems}</strong></div>
      <div class="count-item"><span>Mart Rows</span><strong>${summary.tableauMartItems}</strong></div>
      <div class="count-item"><span>Failures</span><strong>${escapeHtml(summary.failedSteps)}</strong></div>
    </div>
  `;
  els.eventCount.textContent = `${events.length.toLocaleString()} events`;
}

function renderDurationBars(jobs) {
  const visibleJobs = jobs.filter((job) => (
    job.key !== "finish" &&
    job.status !== "skipped" &&
    job.duration > 0.05 &&
    (job.startTime || job.endTime)
  ));
  if (!visibleJobs.length) {
    els.durationBars.innerHTML = `<div class="duration-empty">No timed jobs</div>`;
    return;
  }

  const totalDuration = visibleJobs.reduce((sum, job) => sum + job.duration, 0) || 1;
  const segments = visibleJobs.map((job, index) => {
    const width = Math.max(0.7, (job.duration / totalDuration) * 100);
    const title = `${job.label}: ${fmtSeconds(job.duration)} (${job.status})`;
    const segmentColor = job.status === "success" ? SUCCESS_TONES[index % SUCCESS_TONES.length] : "";
    const colorStyle = segmentColor ? `; --segment-color: ${segmentColor}` : "";
    return `
      <div
        class="duration-segment ${escapeHtml(job.status)}"
        style="flex-basis: ${width}%${colorStyle}"
        title="${escapeHtml(title)}"
        aria-label="${escapeHtml(title)}"
      >
        <span>${escapeHtml(job.label)}</span>
      </div>
    `;
  }).join("");

  const legend = visibleJobs.map((job, index) => {
    const segmentColor = job.status === "success" ? SUCCESS_TONES[index % SUCCESS_TONES.length] : "";
    const colorStyle = segmentColor ? ` style="--segment-color: ${segmentColor}"` : "";
    return `
    <div class="duration-legend-item" title="${escapeHtml(job.label)}">
      <span class="legend-dot ${escapeHtml(job.status)}"${colorStyle}></span>
      <span class="legend-label">${escapeHtml(job.label)}</span>
      <strong>${fmtSeconds(job.duration)}</strong>
    </div>
  `;
  }).join("");

  els.durationBars.innerHTML = `
    <div class="duration-timeline">${segments}</div>
    <div class="duration-legend">${legend}</div>
  `;
}

function renderPipeline(jobs) {
  els.pipeline.innerHTML = PIPELINE_COLUMNS.map((column) => {
    const columnJobs = jobs.filter((job) => job.column === column.id);
    return `
      <section class="pipeline-column">
        <div class="column-title">
          <strong>${escapeHtml(column.label)}</strong>
          <span>${columnJobs.length}</span>
        </div>
        <div class="job-list">
          ${columnJobs.map(renderJob).join("") || `<div class="job unknown"><div class="job-name">No jobs</div></div>`}
        </div>
      </section>
    `;
  }).join("");

  document.querySelectorAll("[data-job-issues]").forEach((button) => {
    button.addEventListener("click", () => {
      issueScope = button.dataset.jobIssues;
      openIssues();
    });
  });
}

function renderJob(job) {
  const issueButton = job.issues.length
    ? `<button class="issue-button" type="button" data-job-issues="${escapeHtml(job.key)}">${job.issues.length} issue(s)</button>`
    : "";
  return `
    <article class="job ${escapeHtml(job.status)}">
      <div class="job-header">
        <div class="job-name">${escapeHtml(job.label)}</div>
        <span class="job-status">${escapeHtml(job.status)}</span>
      </div>
      <div class="job-time">${timeOnly(job.startTime)} -> ${timeOnly(job.endTime)} - ${fmtSeconds(job.duration)}</div>
      ${job.detail ? `<div class="job-detail">${escapeHtml(job.detail)}</div>` : ""}
      ${issueButton}
    </article>
  `;
}

function renderIssues() {
  const source = issueScope
    ? currentJobs.find((job) => job.key === issueScope)?.issues ?? []
    : currentEvents.filter((event) => ["WARNING", "ERROR", "EXCEPTION"].includes(event.level));

  const title = issueScope
    ? `${currentJobs.find((job) => job.key === issueScope)?.label ?? issueScope}: ${source.length} issue(s)`
    : `${source.length} issue(s)`;
  els.issueCount.textContent = title;
  els.issues.innerHTML = source.length
    ? source.slice().reverse().map((event) => `
      <div class="issue">
        <strong class="level-${event.level}">${event.level} - ${escapeHtml(event.message)}</strong>
        <time>${timeOnly(event.timestamp)} - ${escapeHtml(event.logger)}</time>
        ${event.extraText ? `<p>${escapeHtml(event.extraText)}</p>` : ""}
      </div>
    `).join("")
    : `<div class="issue"><strong>No warnings or errors</strong><time>No issue events in this scope.</time></div>`;
}

function openIssues() {
  renderIssues();
  els.issueDrawer.classList.add("open");
  els.issueDrawer.setAttribute("aria-hidden", "false");
}

function closeIssues() {
  els.issueDrawer.classList.remove("open");
  els.issueDrawer.setAttribute("aria-hidden", "true");
  issueScope = null;
}

function renderRows() {
  const level = els.levelFilter.value;
  const keyword = els.search.value.trim().toLowerCase();
  const filtered = currentEvents.filter((event) => {
    const levelOk = level === "all" || event.level === level;
    const textOk = !keyword || event.raw.toLowerCase().includes(keyword);
    return levelOk && textOk;
  });

  els.eventRows.innerHTML = filtered.map((event) => `
    <tr>
      <td>${timeOnly(event.timestamp)}</td>
      <td class="level-${event.level}">${escapeHtml(event.level)}</td>
      <td>${escapeHtml(event.logger)}</td>
      <td>${escapeHtml(event.message)}${event.extraText ? `<br><small>${escapeHtml(event.extraText)}</small>` : ""}</td>
    </tr>
  `).join("");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showNotice(message) {
  els.notice.textContent = message;
  els.notice.classList.toggle("hidden", !message);
}

function clearView() {
  currentEvents = [];
  currentJobs = [];
  renderSummary([]);
  renderDurationBars([]);
  renderPipeline([]);
  renderRows();
}

async function loadDate(dateValue) {
  if (!dateValue) {
    return;
  }

  showNotice("");
  const url = `../logs/app_${dateValue}.log?ts=${Date.now()}`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      clearView();
      showNotice(`Cannot read logs/app_${dateValue}.log. Run the local server from the repository root.`);
      return;
    }

    const text = await response.text();
    currentEvents = selectLatestRun(parseLog(text));
    if (!currentEvents.length) {
      clearView();
      showNotice("The log file was opened, but no parseable events were found.");
      return;
    }

    currentJobs = buildPipelineJobs(currentEvents);
    renderSummary(currentEvents);
    renderDurationBars(currentJobs);
    renderPipeline(currentJobs);
    renderRows();
  } catch (error) {
    clearView();
    showNotice(`Failed to read log: ${error.message}`);
  }
}

els.date.addEventListener("change", () => loadDate(els.date.value));
els.levelFilter.addEventListener("change", renderRows);
els.search.addEventListener("input", renderRows);
els.openIssues.addEventListener("click", () => {
  issueScope = null;
  openIssues();
});
els.closeIssues.addEventListener("click", closeIssues);
els.issueDrawer.addEventListener("click", (event) => {
  if (event.target === els.issueDrawer) {
    closeIssues();
  }
});

els.date.value = todayIso();
loadDate(els.date.value);
