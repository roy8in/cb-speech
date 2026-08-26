const PIPELINE_COLUMNS = [
  { id: "prepare", label: "Prepare" },
  { id: "collect", label: "Collect" },
  { id: "maintenance", label: "Maintenance" },
  { id: "analyze", label: "Analyze" },
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
];

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

function parseExtra(extraText) {
  const result = {};
  if (!extraText) {
    return result;
  }
  for (const part of extraText.split(", ")) {
    const index = part.indexOf("=");
    if (index > 0) {
      result[part.slice(0, index).trim()] = part.slice(index + 1).trim();
    }
  }
  return result;
}

function parseLine(line) {
  const parts = line.split(" | ");
  if (parts.length < 4) {
    return null;
  }
  const [timestamp, level, logger, ...messageParts] = parts;
  const rawMessage = messageParts.join(" | ");
  const divider = rawMessage.indexOf(" | ");
  const message = divider >= 0 ? rawMessage.slice(0, divider) : rawMessage;
  const extraText = divider >= 0 ? rawMessage.slice(divider + 3) : "";
  return {
    timestamp,
    level: level.trim(),
    logger: logger.trim(),
    message: message.trim(),
    extraText: extraText.trim(),
    extra: parseExtra(extraText),
    raw: line,
  };
}

function parseLog(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map(parseLine)
    .filter(Boolean);
}

function isRunStart(event) {
  return ["Starting pipeline run", "Starting sync run"].includes(event.message);
}

function isRunFinish(event) {
  return ["Finished pipeline run", "Finished sync run"].includes(event.message);
}

function selectLatestRun(events) {
  const startIndex = events
    .map((event, index) => ({ event, index }))
    .reverse()
    .find((item) => isRunStart(item.event))?.index;
  if (startIndex === undefined) {
    return events;
  }
  const endOffset = events
    .slice(startIndex)
    .findIndex((event) => isRunFinish(event));
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

function normalizeStatus(status) {
  const value = String(status || "unknown").toLowerCase();
  if (["success", "failed", "running", "skipped", "warning"].includes(value)) {
    return value;
  }
  if (value === "partial") {
    return "warning";
  }
  if (["fail", "error"].includes(value)) {
    return "failed";
  }
  return "unknown";
}

function eventsBetween(events, start, end) {
  const startMs = parseTimestamp(start?.timestamp) ?? -Infinity;
  const endMs = parseTimestamp(end?.timestamp) ?? Infinity;
  return events.filter((event) => {
    const eventMs = parseTimestamp(event.timestamp);
    return eventMs !== null && eventMs >= startMs && eventMs <= endMs;
  });
}

function makeJob(spec, start, end, statusEvent, events) {
  const scopedEvents = eventsBetween(events, start, end ?? start);
  const issues = scopedEvents.filter((event) =>
    ["WARNING", "ERROR", "EXCEPTION"].includes(event.level)
  );
  const duration = Number(statusEvent?.extra.duration_sec);
  return {
    key: spec.key,
    column: spec.column,
    label: spec.label,
    startTime: start?.timestamp ?? "",
    endTime: end?.timestamp ?? start?.timestamp ?? "",
    duration: Number.isFinite(duration)
      ? duration
      : secondsBetween(start?.timestamp, end?.timestamp),
    status: normalizeStatus(statusEvent?.extra.status),
    detail: buildJobDetail(spec.key, statusEvent),
    issues,
  };
}

function buildJobDetail(key, event) {
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
    return [
      `new ${fmtNumber(event.extra.total_new)}`,
      `refreshed ${fmtNumber(event.extra.total_refreshed)}`,
      event.extra.error_message ? `error ${event.extra.error_message}` : "",
    ].filter(Boolean).join(" - ");
  }
  if (key.includes("analysis")) {
    return [
      `analyzed ${fmtNumber(event.extra.analyzed_items)}`,
      event.extra.error_message ? `error ${event.extra.error_message}` : "",
    ].filter(Boolean).join(" - ");
  }
  if (event.extra.error_message) {
    return `error ${event.extra.error_message}`;
  }
  return "";
}

function buildPipelineJobs(events) {
  const jobs = JOB_SPECS.map((spec) => {
    const jobEvents = events.filter((event) =>
      event.message === "Pipeline job status"
      && event.extra.job_name === spec.key
    );
    if (!jobEvents.length) {
      return null;
    }
    const start = jobEvents.find((event) => event.extra.status === "running")
      ?? jobEvents[0];
    const end = [...jobEvents]
      .reverse()
      .find((event) => event.extra.status !== "running") ?? start;
    return makeJob(spec, start, end, end, events);
  }).filter(Boolean);

  const finish = [...events].reverse().find(isRunFinish);
  if (finish) {
    jobs.push({
      key: "finish",
      column: "finish",
      label: "Finish run",
      startTime: finish.timestamp,
      endTime: finish.timestamp,
      duration: Number(finish.extra.duration_sec) || 0,
      status: normalizeStatus(finish.extra.status),
      detail: [
        `new ${fmtNumber(finish.extra.total_new)}`,
        `refreshed ${fmtNumber(finish.extra.total_refreshed)}`,
        `analyzed ${fmtNumber(finish.extra.analyzed_items)}`,
      ].join(" - "),
      issues: [],
    });
  }
  return jobs;
}

function inferStatus(events) {
  const finish = [...events].reverse().find(isRunFinish);
  if (finish?.extra.status) {
    return finish.extra.status;
  }
  if (events.some((event) => ["ERROR", "EXCEPTION"].includes(event.level))) {
    return "failed";
  }
  return "unknown";
}

function renderSummary(events) {
  const start = events.find(isRunStart);
  const finish = [...events].reverse().find(isRunFinish);
  const status = inferStatus(events).toUpperCase();
  els.status.className = `status-pill ${normalizeStatus(status)}`;
  els.status.textContent = status;
  els.runId.textContent = finish?.extra.run_id ?? start?.extra.run_id ?? "-";
  els.started.textContent = timeOnly(start?.timestamp ?? events[0]?.timestamp);
  els.ended.textContent = timeOnly(finish?.timestamp ?? events.at(-1)?.timestamp);
  els.duration.textContent = finish?.extra.duration_sec
    ? fmtSeconds(finish.extra.duration_sec)
    : fmtSeconds(secondsBetween(events[0]?.timestamp, events.at(-1)?.timestamp));
  els.rows.innerHTML = `
    <div class="count-grid">
      <div class="count-item"><span>New</span><strong>${fmtNumber(finish?.extra.total_new)}</strong></div>
      <div class="count-item"><span>Refreshed</span><strong>${fmtNumber(finish?.extra.total_refreshed)}</strong></div>
      <div class="count-item"><span>Analyzed</span><strong>${fmtNumber(finish?.extra.analyzed_items)}</strong></div>
      <div class="count-item"><span>Failures</span><strong>${escapeHtml(finish?.extra.failed_steps || "-")}</strong></div>
    </div>
  `;
  els.eventCount.textContent = `${events.length.toLocaleString()} events`;
}

function renderDurationBars(jobs) {
  const visible = jobs.filter((job) =>
    job.key !== "finish"
    && job.status !== "skipped"
    && job.duration > 0.05
  );
  if (!visible.length) {
    els.durationBars.innerHTML = '<div class="duration-empty">No timed jobs</div>';
    return;
  }
  const total = visible.reduce((sum, job) => sum + job.duration, 0) || 1;
  const segments = visible.map((job) => {
    const width = Math.max(0.7, (job.duration / total) * 100);
    return `
      <div class="duration-segment ${escapeHtml(job.status)}"
           style="flex-basis: ${width}%"
           title="${escapeHtml(job.label)}: ${fmtSeconds(job.duration)}">
        <span>${escapeHtml(job.label)}</span>
      </div>
    `;
  }).join("");
  els.durationBars.innerHTML = `<div class="duration-timeline">${segments}</div>`;
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

function renderPipeline(jobs) {
  els.pipeline.innerHTML = PIPELINE_COLUMNS.map((column) => {
    const columnJobs = jobs.filter((job) => job.column === column.id);
    const body = columnJobs.length
      ? columnJobs.map(renderJob).join("")
      : '<div class="job unknown"><div class="job-name">No jobs</div></div>';
    return `
      <section class="pipeline-column">
        <div class="column-title">
          <strong>${escapeHtml(column.label)}</strong>
          <span>${columnJobs.length}</span>
        </div>
        <div class="job-list">${body}</div>
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

function renderIssues() {
  const source = issueScope
    ? currentJobs.find((job) => job.key === issueScope)?.issues ?? []
    : currentEvents.filter((event) =>
        ["WARNING", "ERROR", "EXCEPTION"].includes(event.level)
      );
  els.issueCount.textContent = `${source.length} issue(s)`;
  els.issues.innerHTML = source.length
    ? source.slice().reverse().map((event) => `
        <div class="issue">
          <strong class="level-${event.level}">${escapeHtml(event.level)} - ${escapeHtml(event.message)}</strong>
          <time>${timeOnly(event.timestamp)} - ${escapeHtml(event.logger)}</time>
          ${event.extraText ? `<p>${escapeHtml(event.extraText)}</p>` : ""}
        </div>
      `).join("")
    : '<div class="issue"><strong>No warnings or errors</strong></div>';
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
      showNotice(`Cannot read logs/app_${dateValue}.log.`);
      return;
    }
    currentEvents = selectLatestRun(parseLog(await response.text()));
    if (!currentEvents.length) {
      clearView();
      showNotice("No parseable events were found.");
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
