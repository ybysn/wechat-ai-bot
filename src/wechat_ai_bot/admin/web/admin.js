"use strict";

const $ = (id) => document.getElementById(id);
const text = (id, value) => { $(id).textContent = value; };
const dialog = $("unlock");
const settingsForms = [...document.querySelectorAll("[data-settings]")];
const fields = Object.fromEntries(settingsForms.flatMap((form) =>
  [...form.elements].filter((field) => field.name).map((field) => [field.name, field])));
const navigation = [...document.querySelectorAll(".sidebar nav a")];
const pages = [...document.querySelectorAll("[data-page]")];
const sidebar = document.querySelector(".sidebar");
const storageKey = "wechat-bot-admin-key";
const labels = {
  native: "Native Daemon", consumer: "消息接收", worker: "观察 Worker",
  sender: "正式 Sender", preflight: "Sender 预检",
};
const activeLabels = { active: "运行中", inactive: "已停止", failed: "故障", activating: "启动中", deactivating: "停止中", unknown: "未知" };
const bootLabels = { enabled: "已启用", disabled: "已停用", static: "按需启动", masked: "已屏蔽", unknown: "未知" };
const pipelineLabels = {
  observing: "接收与决策运行中", receiving_only: "仅接收消息", receiving_paused: "消息接收已停止",
  offline: "微信账号离线", failed: "服务故障", unknown: "运行状态未知",
};
const modelLimits = ["modelDaily", "modelPerMinute", "modelInterval"];
const replyLimits = ["replyHourly", "replyDaily", "replyInterval"];
const settingLimits = [...modelLimits, ...replyLimits];
const formEdits = new Map();
const date = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
});

let sessionKey = "";
let modelNames = { deepseek: "deepseek-flash", ollama: "" };
let refreshing = false;
let readingHistory = false;
let readingGroups = false;
let groupListing = null;
let draftRevision = null;
let savedSettings = null;
let defaultLimits = {};
let savingDraft = false;
let currentPage = "overview";

function timestamp(value, fallback = "暂无记录") {
  const parsed = typeof value === "string" ? new Date(value) : null;
  return parsed && Number.isFinite(parsed.valueOf()) ? date.format(parsed) : fallback;
}

function modelName(provider) {
  return { deepseek: "DeepSeek", ollama: "Ollama" }[provider] || "未知提供方";
}

async function api(path, key = sessionKey, options = {}, timeout = 12000) {
  const response = await fetch(path, {
    ...options,
    headers: { ...options.headers, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    signal: AbortSignal.timeout(timeout),
  });
  const value = await response.json();
  if (!response.ok) {
    const error = new Error(response.status === 401 ? "访问密钥无效，请重新输入。" : value.error);
    error.status = response.status;
    throw error;
  }
  return value;
}

function storeKey(key) {
  try {
    if (key) sessionStorage.setItem(storageKey, key);
    else sessionStorage.removeItem(storageKey);
  } catch {
    // The current page can still be used with browser session storage disabled.
  }
}

function showUnlock(message = "") {
  sessionKey = "";
  draftRevision = null;
  updateSaveButtons();
  storeKey("");
  setMenuOpen(false);
  $("access-key").value = "";
  $("history-list").replaceChildren();
  $("export-history").hidden = true;
  text("history-state", "解锁后查看发送记录。");
  groupListing = null;
  loadDraft(null, {});
  text("group-list-state", "解锁后读取微信群列表。");
  $("unlock-error").hidden = !message;
  text("unlock-error", message);
  if (!dialog.open) dialog.showModal();
  $("access-key").focus();
}

function serviceLabel(key, state) {
  if (state.load === "not-found") return "未部署";
  if (key === "preflight" && state.active === "inactive") return "空闲";
  return activeLabels[state.active] || "未知";
}

function renderRows(values) {
  const rows = Object.entries(labels).map(([key, name]) => {
    const state = values[key] || {};
    const tr = document.createElement("tr");
    const nameCell = document.createElement("th");
    nameCell.scope = "row";
    tr.append(nameCell);
    const statusCell = tr.insertCell();
    const uptimeCell = tr.insertCell();
    const bootCell = tr.insertCell();
    const restartCell = tr.insertCell();
    uptimeCell.dataset.label = "运行时间";
    bootCell.dataset.label = "开机启动";
    restartCell.dataset.label = "重启次数";
    nameCell.textContent = name;
    const pill = document.createElement("span");
    pill.className = `pill ${state.active === "active" ? "good" : state.active === "failed" ? "warn" : "muted"}`;
    pill.textContent = serviceLabel(key, state);
    statusCell.append(pill);
    const minutes = Math.floor(state.uptimeSeconds / 60);
    uptimeCell.textContent = state.uptimeSeconds == null ? "—"
      : minutes < 60 ? `${minutes} 分钟` : `${Math.floor(minutes / 60)} 小时 ${minutes % 60} 分`;
    bootCell.textContent = state.load === "not-found" ? "—" : bootLabels[state.startup] || "未知";
    restartCell.textContent = state.restarts ?? "—";
    return tr;
  });
  $("service-rows").replaceChildren(...rows);
}

function renderPipeline(services, senderStatus) {
  const holds = {
    sender_paused: "已暂停", scheduled_needs_review: "待核对",
    reply_needs_review: "待核对", outbox_unresolved: "待核对",
    sender_configuration_unavailable: "配置异常", native_offline: "等待微信上线",
  };
  for (const key of ["consumer", "worker", "sender"]) {
    const state = services[key] || {};
    const step = $(`${key}-step`);
    const hold = key === "sender" && state.active === "active" ? holds[senderStatus] : null;
    step.textContent = hold || serviceLabel(key, state);
    step.dataset.state = Boolean(hold) || state.active === "failed" ? "warning"
      : state.active === "active" ? "ready" : "unknown";
  }
}

function renderStatus(data) {
  const native = data.native;
  const online = native.authenticated === true && native.connection === "online";
  const reconnecting = native.connection === "reconnecting";
  const stateMap = {
    access_denied: ["权限不足", "本机账号状态读取请求被拒绝"],
    unavailable: ["无法连接", "本机 Native 接口暂不可用"],
    unconfigured: ["缺少配置", "后台需要本机账号配置"],
  };
  const transport = { native_push: "Native 推送", heartbeat_polling: "心跳轮询" }[native.transport];
  const [status, detail] = native.state === "reachable"
    ? online ? ["已登录", transport || "Native 账号在线"]
      : reconnecting ? ["重连中", "Native 正在恢复会话"]
        : native.connection === "disconnected" ? ["未登录", "Native 账号连接已断开"]
          : ["状态未知", "Native 已响应"]
    : stateMap[native.state] || ["状态未知", "Native 当前状态无法确认"];
  text("connection", status);
  text("connection-detail", detail);
  $("wechat-login").hidden = native.state !== "reachable" || online || reconnecting;
  if (online) $("wechat-login-result").hidden = true;
  $("connection-light").className = `status-light ${online ? "online" : "offline"}`;
  text("refreshed", `更新于 ${timestamp(data.checkedAt, "刚刚")} · 北京时间`);
  text("pipeline", pipelineLabels[data.pipeline] || "运行状态未知");

  const model = data.model;
  text("active-model", model.model || "无法读取");
  const source = { runtime: "已部署配置", reference: "本机参考配置" }[model.source];
  text("active-model-source", source ? `${modelName(model.provider)} · ${source}` : "配置不可读");
  const quota = model.dailyLimit == null ? "模型配额未显示"
    : `日上限 ${model.dailyLimit.toLocaleString("zh-CN")} 次 · 最短间隔 ${model.intervalSeconds} 秒 · 每 60 秒 ${model.rollingLimit} 次`;
  text("limits", quota);

  const sender = data.services.sender || {};
  const senderStatus = data.sending.senderStatus;
  const observation = data.sending.mode === "observation" && sender.active !== "active";
  const mode = data.sending.source === "reference" ? "观察模式（参考）" : "观察模式";
  text("sending-mode", sender.active === "active" ? senderStatus === "sender_paused" ? "已暂停发送" : "Sender 运行中"
    : observation ? mode : sender.load === "not-found" ? "未部署" : serviceLabel("sender", sender));
  $("sending-detail").hidden = !observation;
  text("sending-detail", observation ? "接收消息并生成回复建议，自动发送关闭。" : "");
  const policies = {
    unavailable: "后台无法读取生产自动回复策略", inactive: "自动回复策略未生效",
    invalid: "自动回复策略校验失败", active_window: "当前处于自动回复策略窗口",
  };
  const senderStates = {
    sender_paused: "可继续核对已提交的消息",
    waiting_for_schedule: "等待下一发送时段",
    native_offline: "等待微信账号上线",
    scheduled_sent: "定时消息已确认发送",
    reply_sent: "自动回复已确认发送",
    scheduled_needs_review: "定时消息回执待核对",
    reply_needs_review: "自动回复回执待核对",
    outbox_unresolved: "有未决发送记录",
    no_automatic_candidate: "等待符合规则的消息",
    group_send_cooldown: "等待群发送间隔",
    sender_configuration_unavailable: "Sender 配置不可用",
    sender_database_busy: "存储暂时繁忙",
  };
  text("policy", sender.load === "not-found" ? "正式 Sender 未部署"
    : sender.load === "loaded" && sender.active !== "active" ? `正式 Sender · ${activeLabels[sender.active] || "状态未知"}`
      : senderStates[data.sending.senderStatus] || policies[data.sending.policy] || "策略状态未知");
  text("reference-checkin", data.sending.checkinReferenceTime);
  text("cooldown", data.sending.minimumCooldownSeconds == null ? ""
    : `现有发送门禁要求至少间隔 ${data.sending.minimumCooldownSeconds} 秒。`);
  renderRows(data.services);
  renderPipeline(data.services, senderStatus);
  text("last-ack", timestamp(data.activity.lastAck));
  const action = { reply: "建议回复", skip: "跳过回复" }[data.activity.lastDecisionAction];
  text("last-decision", action ? `${action} · ${timestamp(data.activity.lastDecision)}` : timestamp(data.activity.lastDecision));
  text("outbox", data.outbox ? `${data.outbox.queued} / ${data.outbox.sending + data.outbox.needs_review}` : "生产库权限受保护");
}

function setProvider(provider) {
  fields.provider.value = provider;
  fields.model.readOnly = provider === "deepseek";
  fields.model.value = modelNames[provider];
}

function updateTimeFields() {
  fields.replyStart.required = fields.replyEnd.required = fields.replyWindowEnabled.checked;
  fields.checkinTime.required = fields.checkinEnabled.checked;
}

function groupName(id) {
  const groups = groupListing?.groups ?? [];
  const name = groups.find((group) => group.id === id)?.name;
  if (!name || name === id) return id;
  return groups.filter((group) => group.name === name).length > 1 ? `${name}（${id}）` : name;
}

function updateScheduledGroup() {
  const id = savedSettings?.targetGroupId;
  text("scheduled-group", id ? `全局目标群为「${groupName(id)}」。发送时间使用北京时间。`
    : "目标群沿用部署配置，发送时间使用北京时间。");
}

function renderGroupOptions(selected = fields.targetGroupId.value) {
  const configured = groupListing?.configured;
  const description = configured?.source === "runtime" ? "已部署目标群" : "本机参考目标群";
  const options = new Map((groupListing?.groups ?? []).map(({ id }) => [id, groupName(id)]));
  if (configured) options.set(configured.id, groupName(configured.id));
  if (selected && !options.has(selected)) options.set(selected, selected);
  fields.targetGroupId.replaceChildren(new Option("沿用部署配置", ""),
    ...[...options].map(([id, name]) => new Option(name, id)));
  fields.targetGroupId.value = selected;
  text("configured-group", !groupListing ? "等待读取当前配置" : configured
    ? `${description} · ${groupName(configured.id)}` : "无法读取当前配置的目标群。");
  updateScheduledGroup();
}

async function refreshGroups() {
  const key = sessionKey;
  if (!key || readingGroups) return;
  readingGroups = true;
  const button = $("refresh-groups");
  button.disabled = true;
  text("group-list-state", "正在读取微信群列表…");
  try {
    const listing = await api("/api/groups", key);
    if (sessionKey !== key) return;
    groupListing = listing;
    renderGroupOptions();
    const state = {
      ready: listing.groups.length ? `已读取 ${listing.groups.length} 个微信群。` : "账号暂未保存微信群。",
      partial: "仅显示已读取的部分群，请刷新重试。",
      access_denied: "本机凭据无权读取群列表。",
      unconfigured: "请先配置本机微信账号。",
      unavailable: "暂时无法读取群列表，请刷新重试。",
    };
    text("group-list-state", state[listing.state] || state.unavailable);
  } catch (error) {
    if (sessionKey !== key) return;
    if (error.status === 401) showUnlock(error.message);
    else text("group-list-state", "暂时无法读取群列表，请刷新重试。");
  } finally {
    readingGroups = false;
    button.disabled = false;
  }
}

const planRows = () => [...$("message-plans").querySelectorAll("[data-plan]")];
const planField = (row, name) => row.querySelector(`[name="${name}"]`);

function updatePlanList() {
  const rows = planRows();
  $("empty-message-plans").hidden = rows.length !== 0;
  $("add-message-plan").disabled = rows.length >= 20;
  rows.forEach((row, index) => { row.querySelector("[data-plan-name]").textContent = `定时计划 ${index + 1}`; });
}

function readPlan(row) {
  const cadence = planField(row, "cadence").value;
  return {
    enabled: planField(row, "enabled").checked,
    cadence,
    date: cadence === "once" ? planField(row, "date").value : "",
    time: planField(row, "time").value,
    text: planField(row, "text").value,
  };
}

function renderPlanPreview(row, preview) {
  row.querySelector("[data-plan-preview]").textContent = preview?.state === "planned"
    ? `预定时间 ${timestamp(preview.nextAt)}（北京时间），等待部署。`
    : preview?.state === "expired" ? "预定时间已过，请选择新的时间。"
      : preview?.state === "paused" ? "定时消息计划已暂停。" : "保存后查看预定时间。";
}

function addMessagePlan(plan, preview) {
  const row = $("message-plan-template").content.firstElementChild.cloneNode(true);
  if (plan) {
    planField(row, "enabled").checked = plan.enabled;
    for (const name of ["cadence", "date", "time", "text"]) planField(row, name).value = plan[name];
  }
  const showDate = () => {
    const once = planField(row, "cadence").value === "once";
    row.querySelector("[data-plan-date]").hidden = !once;
    planField(row, "date").required = once;
  };
  showDate();
  row.addEventListener("input", () => {
    showDate();
    renderPlanPreview(row);
  });
  row.querySelector("[data-remove-plan]").addEventListener("click", () => {
    row.remove();
    updatePlanList();
    markEdited($("special"));
    $("add-message-plan").focus();
  });
  $("message-plans").append(row);
  renderPlanPreview(row, preview);
  return row;
}

function setDraftMessage(form, message, error = false) {
  const status = form.querySelector(".draft-footer [role=status]");
  status.textContent = message;
  status.classList.toggle("error", error);
}

function updateSaveButtons() {
  for (const form of settingsForms) {
    form.querySelector('button[type="submit"]').disabled = draftRevision === null || savingDraft;
  }
}

function markEdited(form) {
  formEdits.set(form, (formEdits.get(form) ?? 0) + 1);
  if (draftRevision !== null) setDraftMessage(form, "此页有未保存的更改。");
}

function loadDraft(draft, model) {
  settingsForms.forEach((form) => form.reset());
  formEdits.clear();
  modelNames = { deepseek: "deepseek-flash", ollama: "qwen3.5:9b" };
  if (model.provider === "ollama") modelNames.ollama = model.model;
  const settings = draft?.settings;
  if (settings) {
    modelNames[settings.provider] = settings.model;
    for (const name of ["replyStart", "replyEnd", "checkinTime"]) fields[name].value = settings[name];
    fields.replyWindowEnabled.checked = settings.replyWindowEnabled;
    fields.checkinEnabled.checked = settings.checkinEnabled;
  }
  setProvider(settings?.provider || model.provider || "deepseek");
  savedSettings = settings ?? {
    provider: fields.provider.value, model: fields.model.value,
    replyWindowEnabled: fields.replyWindowEnabled.checked,
    replyStart: fields.replyStart.value, replyEnd: fields.replyEnd.value,
    checkinEnabled: fields.checkinEnabled.checked, checkinTime: fields.checkinTime.value,
  };
  renderGroupOptions(settings?.targetGroupId ?? "");
  $("message-plans").replaceChildren();
  const plans = settings?.scheduledMessages ?? (settings?.specialMessage ? [settings.specialMessage] : []);
  const previews = draft?.schedulePreviews ?? [draft?.schedulePreview];
  plans.forEach((plan, index) => addMessagePlan(plan, previews[index]));
  updatePlanList();
  const quotas = { modelDaily: model.dailyLimit, modelPerMinute: model.rollingLimit,
    modelInterval: model.intervalSeconds == null ? null : Math.ceil(model.intervalSeconds) };
  defaultLimits = Object.fromEntries(settingLimits.map((name) => {
    const { min, max, defaultValue } = fields[name];
    const quota = quotas[name];
    return [name, Number.isInteger(quota) && quota >= Number(min) && quota <= Number(max)
      ? quota : Number(defaultValue)];
  }));
  for (const name of settingLimits) fields[name].value = settings?.limits?.[name] ?? defaultLimits[name];
  updateTimeFields();
  for (const form of settingsForms) {
    setDraftMessage(form, draft ? `草稿更新于 ${timestamp(draft.updatedAt)}，部署后生效。`
      : "配置保存在本机，部署后生效。");
  }
}

async function unlock(key) {
  await api("/api/session", key);
  sessionKey = key;
  storeKey(key);
  if (dialog.open) dialog.close();
  $("access-key").value = "";
  $("status-error").hidden = true;
  $("page-title").focus();
  refreshPageData();
  draftRevision = null;
  updateSaveButtons();
  try {
    const [status, draft] = await Promise.allSettled([api("/api/status", key), api("/api/draft", key)]);
    if (sessionKey !== key) return;
    if (status.status === "fulfilled") renderStatus(status.value);
    else {
      $("status-error").hidden = false;
      text("status-error", "已解锁。本机状态暂时无法读取，可以继续编辑配置。");
    }
    if (draft.status === "fulfilled") {
      loadDraft(draft.value, status.value?.model || {});
      draftRevision = draft.value?.revision ?? "";
    } else {
      for (const form of settingsForms) {
        setDraftMessage(form, "暂时无法读取已保存的配置。请刷新页面后重试。", true);
      }
    }
  } finally { updateSaveButtons(); }
}

async function refresh() {
  if (!sessionKey || refreshing) return;
  const current = sessionKey;
  refreshing = true;
  $("refresh").disabled = true;
  try {
    const status = await api("/api/status");
    if (sessionKey === current) {
      renderStatus(status);
      $("status-error").hidden = true;
    }
  } catch (error) {
    if (sessionKey !== current) return;
    if (error.status === 401) showUnlock(error.message);
    else {
      $("status-error").hidden = false;
      text("status-error", "本机状态暂时无法刷新，当前显示上次读取的结果。");
    }
  } finally {
    refreshing = false;
    $("refresh").disabled = false;
  }
}

async function refreshHistory() {
  const key = sessionKey;
  if (!key || readingHistory) return;
  readingHistory = true;
  $("read-history").disabled = true;
  try {
    const report = await api("/api/history", key);
    if (sessionKey !== key) return;
    $("export-history").hidden = report.state === "live";
    const labels = { reply: "AI 回复", scheduled: "定时消息", test: "一次性验收", other: "其他发送" };
    const items = report.entries.map((record) => {
      const item = document.createElement("li");
      const meta = document.createElement("div");
      meta.className = "history-meta";
      const source = document.createElement("span");
      source.className = record.source === "reply" ? "pill good" : "pill muted";
      source.textContent = labels[record.source];
      const time = document.createElement("time");
      if (record.sentAt) time.dateTime = record.sentAt;
      time.textContent = timestamp(record.sentAt, "发送时间未记录");
      const body = document.createElement("p");
      body.textContent = record.text;
      meta.append(source, time);
      item.append(meta, body);
      return item;
    });
    $("history-list").replaceChildren(...items);
    const summary = items.length ? `显示最近 ${items.length} 条已确认发送记录。` : "暂无已确认发送的消息。";
    text("history-state", report.state === "unavailable" ? "请在本机授权读取受保护的生产记录。"
      : report.state === "snapshot" ? `${summary} 快照于 ${timestamp(report.checkedAt)} 更新。` : summary);
  } catch (error) {
    if (sessionKey !== key) return;
    if (error.status === 401) showUnlock(error.message);
    else text("history-state", "暂时无法读取发送记录。请重试。");
  } finally {
    readingHistory = false;
    $("read-history").disabled = false;
  }
}

$("read-history").addEventListener("click", refreshHistory);
$("export-history").addEventListener("click", async () => {
  if (!sessionKey) return;
  $("export-history").disabled = true;
  try {
    await api("/api/history/export", sessionKey, { method: "POST" });
    text("history-state", "请在 Ubuntu 终端完成 sudo 认证，然后点「刷新记录」。");
  } catch (error) {
    if (error.status === 401) showUnlock(error.message);
    else text("history-state", error.status ? error.message : "无法打开读取终端。");
  } finally { $("export-history").disabled = false; }
});

$("unlock-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("unlock-button");
  button.disabled = true;
  try { await unlock($("access-key").value.trim()); }
  catch (error) { showUnlock(error.status ? error.message : "无法连接本机后台。请检查启动命令。"); }
  finally { button.disabled = false; }
});

$("desktop-unlock").addEventListener("click", async () => {
  const button = $("desktop-unlock");
  button.disabled = $("unlock-button").disabled = true;
  text("desktop-unlock", "等待 Ubuntu 桌面确认…");
  try {
    const { key } = await api("/api/unlock", "", { method: "POST" }, 55000);
    await unlock(key);
  } catch (error) {
    showUnlock(error.status ? error.message : "桌面确认未完成，请重试或使用密钥。");
  } finally {
    text("desktop-unlock", "在 Ubuntu 桌面确认");
    button.disabled = $("unlock-button").disabled = false;
  }
});

dialog.addEventListener("cancel", (event) => event.preventDefault());
$("lock").addEventListener("click", () => {
  showUnlock();
  renderStatus({ native: {}, checkedAt: null, model: {}, sending: {}, services: {}, activity: {} });
  text("refreshed", "后台已锁定");
  text("reference-checkin", "09:00");
  $("wechat-login-result").hidden = true;
});
$("wechat-login").addEventListener("click", async () => {
  const button = $("wechat-login");
  const key = sessionKey;
  if (!key) return;
  button.disabled = true;
  $("wechat-login-result").hidden = false;
  text("wechat-login-result", "正在打开 Ubuntu 登录终端…");
  try {
    const result = await api("/api/wechat/login", key, { method: "POST" });
    if (sessionKey !== key) return;
    text("wechat-login-result", result.status === "online" ? "微信账号已在线。"
      : "请在 Ubuntu 登录终端完成确认或扫码。页面会自动刷新状态。");
    if (result.status === "online") await refresh();
  } catch (error) {
    if (sessionKey === key) text("wechat-login-result", error.status ? error.message : "暂时无法打开登录终端。");
  } finally { setTimeout(() => { button.disabled = false; }, 3000); }
});
$("refresh").addEventListener("click", refresh);
$("refresh-groups").addEventListener("click", refreshGroups);
fields.provider.addEventListener("change", () => setProvider(fields.provider.value));
fields.model.addEventListener("input", () => { modelNames[fields.provider.value] = fields.model.value; });
fields.replyWindowEnabled.addEventListener("change", updateTimeFields);
fields.checkinEnabled.addEventListener("change", updateTimeFields);
$("add-message-plan").addEventListener("click", () => {
  const row = addMessagePlan();
  markEdited($("special"));
  updatePlanList();
  planField(row, "text").focus();
});
updateTimeFields();

function readPageSettings(page) {
  const data = { ...savedSettings };
  const applyLimits = (names) => {
    data.limits = { ...(data.limits ?? defaultLimits),
      ...Object.fromEntries(names.map((name) => [name, fields[name].valueAsNumber])) };
  };
  switch (page) {
    case "sending":
      if (fields.targetGroupId.value) data.targetGroupId = fields.targetGroupId.value;
      else delete data.targetGroupId;
      break;
    case "reply":
      data.replyWindowEnabled = fields.replyWindowEnabled.checked;
      data.replyStart = fields.replyStart.value || fields.replyStart.defaultValue;
      data.replyEnd = fields.replyEnd.value || fields.replyEnd.defaultValue;
      applyLimits(replyLimits);
      break;
    case "checkin":
      data.checkinEnabled = fields.checkinEnabled.checked;
      data.checkinTime = fields.checkinTime.value || fields.checkinTime.defaultValue;
      break;
    case "special":
      delete data.specialMessage;
      data.scheduledMessages = planRows().map(readPlan);
      break;
    case "model":
      data.provider = fields.provider.value;
      data.model = fields.model.value.trim();
      applyLimits(modelLimits);
      break;
  }
  return data;
}

async function savePage(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const key = sessionKey;
  if (!key || draftRevision === null || savingDraft) return;
  const edit = formEdits.get(form);
  const data = readPageSettings(form.id);
  const rows = form.id === "special" ? planRows() : [];
  savingDraft = true;
  updateSaveButtons();
  setDraftMessage(form, "正在保存此页配置…");
  try {
    const draft = await api("/api/draft", key, {
      method: "PUT", headers: { "If-Match": `"${draftRevision}"` }, body: JSON.stringify(data),
    });
    if (sessionKey === key) {
      savedSettings = draft.settings;
      draftRevision = draft.revision;
      updateScheduledGroup();
      setDraftMessage(form, formEdits.get(form) === edit
        ? `此页草稿已保存于 ${timestamp(draft.updatedAt)}，部署后生效。`
        : "草稿已保存。此页还有未保存的更改。");
      rows.forEach((row, index) => {
        if (row.isConnected && JSON.stringify(readPlan(row)) === JSON.stringify(data.scheduledMessages[index])) {
          renderPlanPreview(row, draft.schedulePreviews[index]);
        }
      });
    }
  } catch (error) {
    if (sessionKey !== key) return;
    if (error.status === 401) showUnlock(error.message);
    else setDraftMessage(form, error.status ? error.message : "暂时无法保存草稿。请重试。", true);
  } finally {
    savingDraft = false;
    updateSaveButtons();
  }
}

for (const form of settingsForms) {
  form.addEventListener("input", () => markEdited(form));
  form.addEventListener("submit", savePage);
}

function refreshPageData() {
  if (currentPage === "history") refreshHistory();
  if ((currentPage === "sending" || currentPage === "special") && !groupListing) refreshGroups();
}

function setMenuOpen(open) {
  sidebar.dataset.menuOpen = String(open);
  $("menu-toggle").setAttribute("aria-expanded", String(open));
}

$("menu-toggle").addEventListener("click", () => setMenuOpen(sidebar.dataset.menuOpen !== "true"));
sidebar.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && sidebar.dataset.menuOpen === "true") {
    setMenuOpen(false);
    $("menu-toggle").focus();
  }
});
for (const link of [...navigation, document.querySelector(".brand")]) {
  link.addEventListener("click", (event) => {
    if (event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey
        && sidebar.dataset.menuOpen === "true" && link.hash === location.hash) {
      setMenuOpen(false);
      $("page-title").focus();
    }
  });
}

function updateNavigation(focus = false) {
  const linkForPage = navigation.find((link) => link.hash === location.hash) ?? navigation[0];
  currentPage = linkForPage.hash.slice(1);
  const title = linkForPage.textContent.trim();
  for (const page of pages) page.hidden = page.id !== currentPage;
  for (const link of navigation) {
    if (link === linkForPage) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  const showRuntime = currentPage === "overview" || currentPage === "services";
  $("runtime-summary").hidden = $("refresh").hidden = !showRuntime;
  $("draft-indicator").hidden = !$(currentPage).hasAttribute("data-settings");
  text("page-title", title);
  document.title = `${title} · 微信机器人`;
  if (focus) {
    setMenuOpen(false);
    window.scrollTo(0, 0);
    $("page-title").focus();
  }
  refreshPageData();
}

document.querySelector(".skip-nav").addEventListener("click", (event) => {
  event.preventDefault();
  $("page-title").focus();
});
window.addEventListener("hashchange", () => updateNavigation(true));
updateNavigation();
setInterval(() => { if (!document.hidden) refresh(); }, 25000);
renderRows({});
try { sessionKey = sessionStorage.getItem(storageKey) || ""; } catch { /* Use the unlock form. */ }
if (sessionKey) unlock(sessionKey).catch(() => showUnlock());
else showUnlock();
