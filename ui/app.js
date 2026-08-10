const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

const elements = {
  orb: document.querySelector("#voice-orb"),
  stateKicker: document.querySelector("#state-kicker"),
  status: document.querySelector("#status-text"),
  transcript: document.querySelector("#live-transcript"),
  listenToggle: document.querySelector("#listen-toggle"),
  listenLabel: document.querySelector("#listen-label"),
  stopSpeaking: document.querySelector("#stop-speaking"),
  cancelTurn: document.querySelector("#cancel-turn"),
  repeatLast: document.querySelector("#repeat-last"),
  newSession: document.querySelector("#new-session"),
  chatLog: document.querySelector("#chat-log"),
  promptForm: document.querySelector("#prompt-form"),
  promptInput: document.querySelector("#prompt-input"),
  promptSubmit: document.querySelector("#prompt-form button[type='submit']"),
  sessionPill: document.querySelector("#session-pill"),
  sessionLabel: document.querySelector("#session-label"),
  workerEngineSelect: document.querySelector("#worker-engine-select"),
  modelSelect: document.querySelector("#model-select"),
  effortSelect: document.querySelector("#effort-select"),
  fastMode: document.querySelector("#fast-mode"),
  workerHost: document.querySelector("#worker-host"),
  workerHostLabel: document.querySelector("#worker-host-label"),
  relayMode: document.querySelector("#relay-mode"),
  usagePill: document.querySelector("#usage-pill"),
  usageLast: document.querySelector("#usage-last"),
  usageSession: document.querySelector("#usage-session"),
  workspaceForm: document.querySelector("#workspace-form"),
  workspacePreset: document.querySelector("#workspace-preset"),
  workspacePresets: document.querySelector("#workspace-presets"),
  workspaceSave: document.querySelector("#workspace-save"),
  workspaceHostLabel: document.querySelector("#workspace-host-label"),
  audioLevel: document.querySelector("#audio-level"),
  pipelineDetail: document.querySelector("#pipeline-detail"),
  pipelineHost: document.querySelector("#worker-host-step"),
  pipelineWorker: document.querySelector("#worker-engine-step"),
  pipelineSteps: [...document.querySelectorAll("[data-pipeline-step]")],
  settingsOpen: document.querySelector("#settings-open"),
  settingsClose: document.querySelector("#settings-close"),
  settingsBackdrop: document.querySelector("#settings-backdrop"),
  settingsForm: document.querySelector("#settings-form"),
  settingsResult: document.querySelector("#settings-result"),
  testConnection: document.querySelector("#test-connection"),
  speechRate: document.querySelector("#speech-rate"),
  speechRateValue: document.querySelector("#speech-rate-value"),
  diagnostics: document.querySelector("#diagnostic-log"),
  toast: document.querySelector("#toast"),
};

let settings;
let view;
let toastTimer;

await listen("roadie://state", ({ payload }) => {
  view = payload;
  renderView();
});

await listen("roadie://settings", ({ payload }) => {
  settings = payload;
  populateSettings();
  renderView();
});

await listen("roadie://diagnostic", ({ payload }) => {
  const time = new Date().toLocaleTimeString();
  const previous = elements.diagnostics.textContent === "Waiting for events…" ? "" : elements.diagnostics.textContent;
  elements.diagnostics.textContent = `${previous}[${time}] ${payload}\n`.slice(-8000);
  elements.diagnostics.scrollTop = elements.diagnostics.scrollHeight;
});

try {
  const bootstrap = await invoke("bootstrap");
  settings = bootstrap.settings;
  view = bootstrap.view;
  populateSettings();
  renderView();
  if ((!settings.localWorker && !settings.sshTarget) || !settings.localWorkspace) openSettings();
} catch (error) {
  showToast(String(error));
}

elements.listenToggle.addEventListener("click", async () => {
  await call(view?.listening ? "stop_listening" : "start_listening");
});
elements.stopSpeaking.addEventListener("click", () => call("stop_speaking"));
elements.cancelTurn.addEventListener("click", () => call("cancel_turn"));
elements.repeatLast.addEventListener("click", () => call("repeat_last"));
elements.newSession.addEventListener("click", () => call("new_session"));
elements.workerEngineSelect.addEventListener("change", () => saveAgentProfile({ localWorkerEngine: elements.workerEngineSelect.value }));
elements.modelSelect.addEventListener("change", () => saveAgentProfile({ model: elements.modelSelect.value }));
elements.effortSelect.addEventListener("change", () => saveAgentProfile({ effort: elements.effortSelect.value }));
elements.fastMode.addEventListener("click", () => saveAgentProfile({ fastMode: !settings.fastMode }));
elements.workerHost.addEventListener("click", () => saveAgentProfile({ localWorker: !settings.localWorker }));
elements.relayMode.addEventListener("click", () => saveAgentProfile({ deepRelay: !settings.deepRelay }));
elements.workspaceForm.addEventListener("submit", saveWorkspacePreset);
elements.settingsOpen.addEventListener("click", openSettings);
elements.settingsClose.addEventListener("click", closeSettings);
elements.settingsBackdrop.addEventListener("click", (event) => {
  if (event.target === elements.settingsBackdrop) closeSettings();
});

elements.promptForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const prompt = elements.promptInput.value.trim();
  if (!prompt) return;
  elements.promptInput.value = "";
  await call("send_text_prompt", { prompt });
});

elements.speechRate.addEventListener("input", () => {
  elements.speechRateValue.value = Number(elements.speechRate.value).toFixed(2);
});

elements.settingsForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await saveSettings();
});

elements.testConnection.addEventListener("click", async () => {
  if (!(await saveSettings(false))) return;
  setSettingsResult(settings.localWorker ? "Testing the Mac worker and optional device/SSH tools…" : "Testing key-only SSH and remote Codex…", "");
  try {
    const result = await invoke("test_connection");
    setSettingsResult(result.output || (result.success ? "Connection succeeded." : "Connection failed."), result.success ? "success" : "error");
  } catch (error) {
    setSettingsResult(String(error), "error");
  }
});

function renderView() {
  if (!view) return;
  const localClaude = settings?.localWorker && activeWorkerEngine() === "claude";
  elements.status.textContent = view.status;
  elements.transcript.textContent = view.liveTranscript || "Say “Hey Codex” or “Hey Claude” followed by a task.";
  elements.transcript.classList.toggle("active", Boolean(view.liveTranscript));
  elements.listenLabel.textContent = view.listening ? "Pause listening" : "Start listening";
  elements.listenToggle.classList.toggle("active", view.listening);
  elements.cancelTurn.disabled = false;
  elements.stopSpeaking.disabled = false;
  elements.promptInput.placeholder = view.connectivity === "reconnecting"
    ? "Add to the saved request during reconnect…"
    : (view.activeWorkers || 0) > 1
    ? "Name the worker number, or say ‘in parallel’ for another worker…"
    : view.working
    ? "Add to this task, or say ‘in parallel’ for another worker…"
    : "Type a prompt while parked…";
  elements.promptSubmit.textContent = view.connectivity === "reconnecting" ? "Save" : view.working ? "Add" : "Send";

  elements.orb.classList.toggle("listening", view.listening && !view.working);
  elements.orb.classList.toggle("working", view.working);
  elements.orb.classList.toggle("speaking", view.speaking);
  elements.stateKicker.textContent = view.connectivity === "reconnecting" ? "RECONNECTING SAFELY" : (view.activeWorkers || 0) > 1 ? `${view.activeWorkers} PARALLEL WORKERS` : view.working && settings?.deepRelay ? "WORKER ↔ LOCAL VOICE" : view.working && settings?.localWorker ? localClaude ? "MAC CLAUDE WORKER" : "MAC CODEX WORKER" : view.working ? "SSH CODEX WORKER" : view.speaking ? "PLAYBACK" : view.listening ? "MICROPHONE LIVE" : "STANDBY";

  const session = view.sessionId;
  const reconnecting = view.connectivity === "reconnecting";
  elements.sessionPill.classList.toggle("connected", Boolean(session) && !reconnecting);
  elements.sessionPill.classList.toggle("reconnecting", reconnecting);
  elements.sessionLabel.textContent = reconnecting
    ? `Saved · retry ${view.reconnectAttempt || 1}${view.queuedAdditions ? ` · +${view.queuedAdditions}` : ""}`
    : (view.activeWorkers || 0) > 1
      ? `${view.activeWorkers} workers active`
    : session
      ? `Session ${session.slice(0, 8)}`
      : "Fresh session";
  const lockedProfile = localClaude;
  elements.workerEngineSelect.disabled = view.working || !settings?.localWorker;
  elements.modelSelect.disabled = view.working || lockedProfile;
  elements.effortSelect.disabled = view.working || lockedProfile;
  elements.fastMode.disabled = view.working || lockedProfile;
  elements.workerHost.disabled = view.working;
  elements.relayMode.disabled = view.working;
  elements.workspacePreset.disabled = view.working;
  elements.workspaceSave.disabled = view.working;
  elements.pipelineHost.textContent = settings?.localWorker ? "Mac" : "SSH";
  elements.pipelineWorker.textContent = localClaude ? "Claude" : settings?.codexReasoningEffort ? settings.codexReasoningEffort.toUpperCase() : "Agent";
  renderPipeline();
  renderUsage();
  renderMessages(view.messages || []);
}

function renderPipeline() {
  const stage = view.pipelineStage || "loading";
  const activeIndex = {
    loading: 1,
    capturing: 0,
    transcribing: 1,
    transferring: 2,
    processing: 3,
    reporting: 4,
    responding: 5,
    reconnecting: 2,
  }[stage] ?? -1;
  elements.pipelineSteps.forEach((step, index) => {
    step.classList.toggle("active", index === activeIndex);
    step.classList.toggle("complete", activeIndex > index);
    step.classList.toggle("ready", stage === "listening" && index === 0);
    step.classList.toggle("error", stage === "error" && index === activeIndex);
  });
  elements.pipelineDetail.textContent = view.pipelineDetail || "Voice pipeline is starting";
  elements.audioLevel.style.width = `${Math.round(Math.max(0, Math.min(1, view.audioLevel || 0)) * 100)}%`;
}

function renderUsage() {
  const last = view.lastUsage;
  const lastReporter = view.lastReporterUsage;
  const session = view.sessionUsage || {};
  const reporterSession = view.sessionReporterUsage || {};
  if (last) {
    const reporter = lastReporter ? ` · V ${compact(lastReporter.inputTokens + lastReporter.outputTokens)}` : "";
    elements.usageLast.textContent = `W ${compact(last.inputTokens)} in · ${compact(last.outputTokens)} out${reporter}`;
  } else {
    elements.usageLast.textContent = "Last —";
  }
  const workerSessionTotal = (session.inputTokens || 0) + (session.outputTokens || 0);
  const reporterSessionTotal = (reporterSession.inputTokens || 0) + (reporterSession.outputTokens || 0);
  const sessionTotal = workerSessionTotal + reporterSessionTotal;
  elements.usageSession.textContent = sessionTotal ? `${compact(sessionTotal)} session · ${compact(reporterSessionTotal)} voice` : "Session —";
  const workerLocation = settings?.localWorker ? activeWorkerEngine() === "claude" ? "Mac Claude Code worker" : "Mac Codex worker" : "SSH Codex worker";
  elements.usagePill.title = last
    ? `${workerLocation} — last: ${exact(last.inputTokens)} input (${exact(last.cachedInputTokens)} cached), ${exact(last.outputTokens)} output, ${exact(last.reasoningOutputTokens)} reasoning. Local reporter — last: ${exact(lastReporter?.inputTokens)} input (${exact(lastReporter?.cachedInputTokens)} cached), ${exact(lastReporter?.outputTokens)} output, ${exact(lastReporter?.reasoningOutputTokens)} reasoning. Combined session: ${exact(sessionTotal)} tokens (${exact(reporterSessionTotal)} local reporter).`
    : "Worker token usage appears after the first completed turn.";
}

function renderMessages(messages) {
  if (!messages.length) {
    elements.chatLog.innerHTML = '<div class="empty-chat"><span>No turns yet</span><p>Your spoken requests and worker responses will appear here.</p></div>';
    return;
  }

  const wasNearBottom = elements.chatLog.scrollHeight - elements.chatLog.scrollTop - elements.chatLog.clientHeight < 90;
  const fragment = document.createDocumentFragment();
  for (const message of messages) {
    const article = document.createElement("article");
    article.className = `message ${message.role}`;
    const avatar = document.createElement("div");
    avatar.className = "message-avatar";
    const localClaude = settings?.localWorker && activeWorkerEngine() === "claude";
    avatar.textContent = message.role === "reporter" ? "VO" : message.role === "worker" || message.role === "agent" ? localClaude ? "CL" : "CX" : message.role === "user" ? "YOU" : "!";
    const body = document.createElement("div");
    const role = document.createElement("div");
    role.className = "message-role";
    role.textContent = message.role === "reporter" ? "Local voice reporter · no tools" : message.role === "worker" || message.role === "agent" ? settings?.localWorker ? localClaude ? "Mac Claude Code worker" : "Mac Codex worker" : "SSH Codex worker" : message.role;
    const text = document.createElement("pre");
    text.className = "message-text";
    text.textContent = message.text;
    body.append(role, text);
    article.append(avatar, body);
    fragment.append(article);
  }
  elements.chatLog.replaceChildren(fragment);
  if (wasNearBottom || messages.length < 4) elements.chatLog.scrollTop = elements.chatLog.scrollHeight;
}

function populateSettings() {
  const fields = elements.settingsForm.elements;
  fields.localWorkspace.value = settings.localWorkspace;
  fields.sshTarget.value = settings.sshTarget;
  fields.workspace.value = settings.workspace;
  fields.codexExecutable.value = settings.codexExecutable;
  fields.localCodexExecutable.value = settings.localCodexExecutable;
  fields.localClaudeExecutable.value = settings.localClaudeExecutable;
  fields.reporterModel.value = settings.reporterModel;
  fields.sandbox.value = settings.sandbox;
  fields.wakePhrase.value = settings.wakePhrase;
  fields.locale.value = settings.locale;
  fields.speechRate.value = settings.speechRate;
  fields.silenceMs.value = String(settings.silenceMs);
  fields.voiceIsolation.checked = settings.voiceIsolation !== false;
  fields.autoListen.checked = settings.autoListen;
  elements.speechRateValue.value = Number(settings.speechRate).toFixed(2);
  renderAgentProfile();
}

function renderAgentProfile() {
  const engine = activeWorkerEngine();
  const localClaude = settings.localWorker && engine === "claude";
  elements.workerEngineSelect.value = settings.localWorker ? engine : "codex";
  const model = localClaude ? "claude-opus-5" : settings.codexModel || "";
  if (![...elements.modelSelect.options].some((option) => option.value === model)) {
    elements.modelSelect.add(new Option(model, model));
  }
  elements.modelSelect.value = model;
  elements.effortSelect.value = localClaude ? "xhigh" : settings.codexReasoningEffort || "";
  const fast = localClaude ? false : Boolean(settings.fastMode);
  elements.fastMode.classList.toggle("active", fast);
  elements.fastMode.setAttribute("aria-pressed", String(fast));
  elements.workerHost.classList.toggle("active", Boolean(settings.localWorker));
  elements.workerHost.setAttribute("aria-pressed", String(Boolean(settings.localWorker)));
  elements.workerHostLabel.textContent = settings.localWorker ? "Mac" : "SSH";
  elements.workerHost.title = settings.localWorker
    ? "Use the optional key-only SSH worker"
    : "Use the Mac-local worker";
  elements.relayMode.classList.toggle("active", Boolean(settings.deepRelay));
  elements.relayMode.setAttribute("aria-pressed", String(Boolean(settings.deepRelay)));
  elements.relayMode.title = "Use a separate local Codex model to narrate worker updates";
  const fields = elements.settingsForm.elements;
  fields.sshTarget.required = !settings.localWorker;
  fields.workspace.required = !settings.localWorker;
  fields.codexExecutable.required = !settings.localWorker;
  fields.sandbox.value = settings.sandbox;
  fields.sandbox.disabled = false;
  elements.testConnection.textContent = settings.localWorker ? localClaude ? "Save & test Claude QA" : "Save & test Mac QA" : "Save & test SSH";
  renderWorkspacePresets();
  renderWorkspacePreset();
}

function activeWorkerEngine() {
  return settings?.localWorkerEngine === "claude" ? "claude" : "codex";
}

function renderWorkspacePreset() {
  const local = Boolean(settings.localWorker);
  elements.workspaceHostLabel.textContent = local ? "MAC WORKDIR" : "SSH WORKDIR";
  elements.workspacePreset.value = local ? settings.localWorkspace : settings.workspace;
  elements.workspacePreset.title = local
    ? `Working directory used by the Mac-local ${activeWorkerEngine() === "claude" ? "Claude Code" : "Codex"} worker`
    : "Working directory used by the remote SSH Codex worker";
}

function renderWorkspacePresets() {
  const presets = [
    [settings.localWorkspace, "Mac workspace"],
    [settings.workspace, "SSH workspace"],
  ];
  const seen = new Set();
  const options = [];
  for (const [value, label] of presets) {
    if (!value || seen.has(value)) continue;
    seen.add(value);
    const option = document.createElement("option");
    option.value = value;
    option.label = label;
    options.push(option);
  }
  elements.workspacePresets.replaceChildren(...options);
}

function readSettings() {
  const fields = elements.settingsForm.elements;
  return {
    localWorkspace: fields.localWorkspace.value.trim(),
    sshTarget: fields.sshTarget.value.trim(),
    workspace: fields.workspace.value.trim(),
    codexExecutable: fields.codexExecutable.value.trim(),
    localCodexExecutable: fields.localCodexExecutable.value.trim(),
    localClaudeExecutable: fields.localClaudeExecutable.value.trim(),
    localWorkerEngine: activeWorkerEngine(),
    reporterModel: fields.reporterModel.value,
    codexModel: settings.codexModel || "",
    codexReasoningEffort: settings.codexReasoningEffort || "",
    fastMode: Boolean(settings.fastMode),
    deepRelay: Boolean(settings.deepRelay),
    localWorker: Boolean(settings.localWorker),
    sandbox: fields.sandbox.value,
    wakePhrase: fields.wakePhrase.value.trim(),
    locale: fields.locale.value.trim(),
    speechRate: Number(fields.speechRate.value),
    silenceMs: Number(fields.silenceMs.value),
    voiceIsolation: fields.voiceIsolation.checked,
    autoListen: fields.autoListen.checked,
  };
}

async function saveAgentProfile(change) {
  if (view?.working) {
    renderAgentProfile();
    showToast("Cancel the active worker turn before changing its profile");
    return;
  }
  const next = {
    ...settings,
    localWorkerEngine: change.localWorkerEngine ?? activeWorkerEngine(),
    codexModel: change.model ?? settings.codexModel ?? "",
    codexReasoningEffort: change.effort ?? settings.codexReasoningEffort ?? "",
    fastMode: change.fastMode ?? Boolean(settings.fastMode),
    deepRelay: change.deepRelay ?? Boolean(settings.deepRelay),
    localWorker: change.localWorker ?? Boolean(settings.localWorker),
  };
  if (change.localWorker === false) {
    next.sandbox = "workspace-write";
  }
  if (next.localWorker && next.localWorkerEngine === "claude") {
    if (next.sandbox !== "danger-full-access") {
      openSettings();
      elements.settingsForm.elements.sandbox.focus();
      showToast("Claude Code requires explicitly selecting Full Mac access");
      renderAgentProfile();
      return;
    }
  }
  try {
    await invoke("save_app_settings", { settings: next });
    settings = next;
    renderAgentProfile();
    const model = next.localWorkerEngine === "claude" ? "Claude Code" : next.codexModel || "Codex default";
    const effort = next.localWorkerEngine === "claude" ? "xhigh" : next.codexReasoningEffort || "default effort";
    showToast(`${model} · ${effort} · ${next.fastMode ? "Fast" : "standard"} · fresh session`);
  } catch (error) {
    renderAgentProfile();
    showToast(String(error));
  }
}

async function saveWorkspacePreset(event) {
  event.preventDefault();
  if (view?.working) {
    renderWorkspacePreset();
    showToast("Cancel the active worker turn before changing its working directory");
    return;
  }
  const workspace = elements.workspacePreset.value.trim();
  const field = settings.localWorker ? "localWorkspace" : "workspace";
  const next = { ...settings, [field]: workspace };
  try {
    await invoke("save_app_settings", { settings: next });
    settings = next;
    populateSettings();
    const host = next.localWorker ? "Mac" : "SSH";
    showToast(`${host} workdir set · fresh session`);
  } catch (error) {
    renderWorkspacePreset();
    showToast(String(error));
  }
}

async function saveSettings(close = true) {
  const next = readSettings();
  if (next.localWorker && next.localWorkerEngine === "claude" && next.sandbox !== "danger-full-access") {
    setSettingsResult("Claude Code requires explicitly selecting Full Mac access.", "error");
    return false;
  }
  if (next.sandbox === "danger-full-access" && settings.sandbox !== "danger-full-access") {
    const approved = window.confirm("Full Mac access lets the selected coding agent read and change anything available to your account without interactive approvals. Enable it?");
    if (!approved) {
      elements.settingsForm.elements.sandbox.value = settings.sandbox;
      return false;
    }
  }
  try {
    await invoke("save_app_settings", { settings: next });
    settings = next;
    populateSettings();
    setSettingsResult("Settings saved.", "success");
    showToast("Road link saved");
    if (close) setTimeout(closeSettings, 280);
    return true;
  } catch (error) {
    setSettingsResult(String(error), "error");
    return false;
  }
}

async function call(command, args = {}) {
  try {
    await invoke(command, args);
  } catch (error) {
    showToast(String(error));
  }
}

function openSettings() {
  elements.settingsBackdrop.hidden = false;
  setTimeout(() => (settings?.localWorker ? elements.settingsForm.elements.localWorkspace : elements.settingsForm.elements.sshTarget).focus(), 60);
}

function closeSettings() {
  elements.settingsBackdrop.hidden = true;
}

function setSettingsResult(message, kind) {
  elements.settingsResult.textContent = message;
  elements.settingsResult.className = `settings-result ${kind}`;
}

function showToast(message) {
  clearTimeout(toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.add("show");
  toastTimer = setTimeout(() => elements.toast.classList.remove("show"), 3200);
}

function compact(value = 0) {
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function exact(value = 0) {
  return new Intl.NumberFormat().format(value);
}
