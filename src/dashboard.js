'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn, execFile } = require('child_process');
const amqp = require('amqplib');
const { config, timestamp, diagnoseAmqpError } = require('./config');

const PORT = Number.parseInt(process.env.UI_PORT || '3000', 10);
const HOST = process.env.UI_HOST || '127.0.0.1';
const PROJECT_ROOT = path.resolve(__dirname, '..');
const PUBLIC_DIR = path.join(PROJECT_ROOT, 'public');
const MAX_LOG_LINES = 250;
const CONSUMERS = ['consumer-1', 'consumer-2'];

// State real-time yang sepenuhnya dibentuk dari data nyata (Docker, RabbitMQ, stdout, disk)
const state = {
  config: {
    queueName: config.queueName,
    maskedUrl: config.maskedUrl,
    singleActiveConsumer: config.singleActiveConsumer,
    prefetchCount: config.prefetchCount,
    processDelayMs: config.processDelayMs,
    reconnectIntervalMs: config.reconnectIntervalMs,
    idempotencyDir: config.idempotencyDir,
  },
  broker: {
    online: false,
    queueExists: false,
    messageCount: 0,
    consumerCount: 0,
    lastError: null,
    checkedAt: null,
  },
  docker: {
    available: false,
    error: null,
    containers: {
      'consumer-1': createEmptyContainerState('consumer-1'),
      'consumer-2': createEmptyContainerState('consumer-2'),
    },
  },
  producer: {
    running: false,
    mode: null,
    pid: null,
  },
  idempotency: {
    count: 0,
    recent: [],
  },
  messages: {}, // keyed by messageId
  currentBatchId: null,
  logs: {
    'consumer-1': [],
    'consumer-2': [],
    producer: [],
  },
};

function createEmptyContainerState(name) {
  return {
    name,
    exists: false,
    running: false,
    status: 'unknown',
    startedAt: null,
    inFlightMessageId: null,
    ackedCount: 0,
    duplicateSkipCount: 0,
    nackCount: 0,
    hasFailover: false,
    connectedToBroker: false,
    explicitlyDisconnected: false,
  };
}

// SSE client registry
const sseClients = new Set();

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {
      sseClients.delete(res);
    }
  }
}

function broadcastSnapshot() {
  broadcast('state', getClientState());
}

function getClientState() {
  // Urutkan pesan untuk batch aktif (atau 10 pesan terbaru)
  const allMessages = Object.values(state.messages);
  let visibleMessages = allMessages;
  if (state.currentBatchId) {
    visibleMessages = allMessages.filter((m) => m.batchId === state.currentBatchId);
  }
  visibleMessages.sort((a, b) => (a.orderNumber || 0) - (b.orderNumber || 0));

  return {
    config: state.config,
    broker: state.broker,
    docker: state.docker,
    producer: state.producer,
    idempotency: state.idempotency,
    currentBatchId: state.currentBatchId,
    messages: visibleMessages,
  };
}

// Parsing baris log terstruktur: [HH:MM:SS.mmm] [ACTOR] [LEVEL] teks...
const LOG_LINE_REGEX = /^\[([^\]]+)\]\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s*(.*)$/;

function parseLogLine(channel, rawLine) {
  const cleaned = rawLine.replace(/\r$/, '');
  const match = cleaned.match(LOG_LINE_REGEX);
  if (match) {
    return {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      channel,
      time: match[1],
      actor: match[2],
      level: match[3].toUpperCase(),
      message: match[4],
      raw: cleaned,
    };
  }
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    channel,
    time: timestamp(),
    actor: channel.toUpperCase(),
    level: 'RAW',
    message: cleaned,
    raw: cleaned,
  };
}

function appendLog(channel, rawLine, { emitState = true } = {}) {
  const text = String(rawLine || '').trimEnd();
  if (!text) return;

  const entry = parseLogLine(channel, text);
  const bucket = state.logs[channel];
  if (bucket) {
    bucket.push(entry);
    if (bucket.length > MAX_LOG_LINES) {
      bucket.splice(0, bucket.length - MAX_LOG_LINES);
    }
  }

  const stateChanged = updateDerivedStateFromLog(entry);
  broadcast('log', entry);
  if (emitState && stateChanged) {
    broadcastSnapshot();
  }
}

function appendSystemLog(level, message) {
  const line = `[${timestamp()}] [SYSTEM] [${level}] ${message}`;
  appendLog('producer', line);
}

function extractBatchId(messageId) {
  // Format default: KRS-<batchId>-<seq>
  const match = String(messageId || '').match(/^KRS-(.+)-(\d{3})$/);
  return match ? match[1] : 'unknown';
}

function ensureMessageEntry(messageId, defaults = {}) {
  const batchId = defaults.batchId || extractBatchId(messageId);
  if (batchId && batchId !== 'unknown') {
    state.currentBatchId = batchId;
  }
  if (!state.messages[messageId]) {
    state.messages[messageId] = {
      messageId,
      batchId,
      orderNumber: defaults.orderNumber || 0,
      nim: defaults.nim || '-',
      mataKuliah: defaults.mataKuliah || '-',
      status: 'QUEUED',
      worker: null,
      previousWorker: null,
      redelivered: false,
      nackedOnce: false,
      duplicateSkipped: false,
      updatedAt: timestamp(),
    };
  } else {
    const existing = state.messages[messageId];
    if (defaults.orderNumber) existing.orderNumber = defaults.orderNumber;
    if (defaults.nim && defaults.nim !== '-') existing.nim = defaults.nim;
    if (defaults.mataKuliah && defaults.mataKuliah !== '-') existing.mataKuliah = defaults.mataKuliah;
    existing.updatedAt = timestamp();
  }
  return state.messages[messageId];
}

function updateDerivedStateFromLog(entry) {
  let changed = false;
  const { channel, level, message } = entry;

  // 1. Producer SENT
  if (channel === 'producer' && level === 'SENT') {
    const m = message.match(/\[(\d+)\/\d+\]\s+Terkirim[^:]*:\s+(\S+)\s+\|\s+NIM=([^|]+)\|\s+MK="([^"]+)"/);
    if (m) {
      const orderNumber = Number.parseInt(m[1], 10);
      const messageId = m[2];
      const nim = m[3].trim();
      const mataKuliah = m[4].trim();
      const msgEntry = ensureMessageEntry(messageId, { orderNumber, nim, mataKuliah });
      if (msgEntry.status !== 'ACKED' && msgEntry.status !== 'PROCESSING') {
        msgEntry.status = 'QUEUED';
      }
      changed = true;
    }
    return changed;
  }

  // 2. Consumer events
  if (CONSUMERS.includes(channel)) {
    const cState = state.docker.containers[channel];
    if (!cState) return false;

    if (level === 'READY' || level === 'STANDBY' || level === 'WAITING') {
      cState.connectedToBroker = true;
      cState.explicitlyDisconnected = false;
      changed = true;
    } else if (level === 'DISCONNECTED' || level === 'RECONNECT') {
      cState.connectedToBroker = false;
      cState.explicitlyDisconnected = true;
      if (cState.inFlightMessageId) {
        const msgEntry = state.messages[cState.inFlightMessageId];
        if (msgEntry && msgEntry.status === 'PROCESSING') {
          msgEntry.status = 'REQUEUED';
        }
        cState.inFlightMessageId = null;
      }
      changed = true;
    } else if (level === 'RECEIVE') {
      const m = message.match(
        /Menerima Pesan #(\d+)\s+\(([^)]+)\)\s+\|\s+NIM:\s+([^|]+)\|\s+MK:\s+([^|]+)\|\s+redelivered=(true|false)/
      );
      if (m) {
        const orderNumber = Number.parseInt(m[1], 10);
        const messageId = m[2];
        const nim = m[3].trim();
        const mataKuliah = m[4].trim();
        const redelivered = m[5] === 'true';
        const msgEntry = ensureMessageEntry(messageId, { orderNumber, nim, mataKuliah });
        if (msgEntry.worker && msgEntry.worker !== channel) {
          msgEntry.previousWorker = msgEntry.worker;
        }
        msgEntry.worker = channel;
        msgEntry.status = 'PROCESSING';
        msgEntry.redelivered = msgEntry.redelivered || redelivered;
        cState.inFlightMessageId = messageId;
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        if (redelivered && channel === 'consumer-2') {
          cState.hasFailover = true;
        }
        changed = true;
      }
    } else if (level === 'FAILOVER') {
      const m = message.match(/Pesan #(\d+)\s+\(([^)]+)\)/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.redelivered = true;
        msgEntry.worker = channel;
        msgEntry.status = 'PROCESSING';
        cState.hasFailover = true;
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        changed = true;
      }
    } else if (level === 'PROCESS') {
      const m = message.match(/Memproses Pesan #(\d+)\s+\(([^)]+)\)/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.worker = channel;
        msgEntry.status = 'PROCESSING';
        cState.inFlightMessageId = m[2];
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        changed = true;
      }
    } else if (level === 'ACK') {
      const m = message.match(/Pesan #(\d+)\s+\(([^)]+)\)\s+BERHASIL/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.worker = channel;
        msgEntry.status = 'ACKED';
        cState.ackedCount += 1;
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        if (cState.inFlightMessageId === m[2]) {
          cState.inFlightMessageId = null;
        }
        changed = true;
      }
    } else if (level === 'NACK') {
      const m = message.match(/Pesan #(\d+)\s+\(([^)]+)\)/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.status = 'NACKED';
        msgEntry.nackedOnce = true;
        cState.nackCount += 1;
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        if (cState.inFlightMessageId === m[2]) {
          cState.inFlightMessageId = null;
        }
        changed = true;
      }
    } else if (level === 'IDEMPOTENCY') {
      const m = message.match(/Pesan #(\d+)\s+\(([^)]+)\)/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.status = 'IDEMPOTENT_SKIP';
        msgEntry.duplicateSkipped = true;
        msgEntry.worker = channel;
        cState.duplicateSkipCount += 1;
        cState.connectedToBroker = true;
        cState.explicitlyDisconnected = false;
        if (cState.inFlightMessageId === m[2]) {
          cState.inFlightMessageId = null;
        }
        changed = true;
      }
    } else if (level === 'CRASH-SIM') {
      const m = message.match(/Pesan #(\d+)\s+\(([^)]+)\)/);
      if (m) {
        const msgEntry = ensureMessageEntry(m[2], { orderNumber: Number.parseInt(m[1], 10) });
        msgEntry.previousWorker = channel;
        msgEntry.status = 'REQUEUED';
        cState.inFlightMessageId = null;
        changed = true;
      }
    }
  }

  return changed;
}

// ==============================================================================
// 1. REAL-TIME DOCKER LOG STREAMERS & CONTAINER STATUS POLLING
// ==============================================================================
const logStreamProcs = new Map();
const lastAttachedStartedAt = new Map();

function attachDockerLogStream(containerName, startedAt) {
  if (logStreamProcs.has(containerName)) {
    return;
  }

  const prevStartedAt = lastAttachedStartedAt.get(containerName);
  const args = ['logs', '-f'];

  if (!prevStartedAt) {
    // Lampiran awal saat dashboard baru dibuka
    args.push('--tail', '80');
  } else if (startedAt) {
    // Container baru dinyalakan ulang -> ambil tepat sejak start (minus 1 detik) agar tidak duplikasi log lama
    const sinceDate = new Date(new Date(startedAt).getTime() - 1000);
    if (!Number.isNaN(sinceDate.getTime())) {
      args.push('--since', sinceDate.toISOString());
    } else {
      args.push('--tail', '30');
    }
  } else {
    args.push('--tail', '30');
  }

  args.push(containerName);
  lastAttachedStartedAt.set(containerName, startedAt || new Date().toISOString());

  const proc = spawn('docker', args, {
    cwd: PROJECT_ROOT,
    shell: false,
    windowsHide: true,
  });

  logStreamProcs.set(containerName, proc);

  let stdoutBuf = '';
  let stderrBuf = '';

  const flushLines = (chunk, isErr) => {
    const str = chunk.toString('utf8');
    if (isErr) {
      stderrBuf += str;
      const lines = stderrBuf.split('\n');
      stderrBuf = lines.pop();
      for (const line of lines) {
        if (line.trim()) appendLog(containerName, line);
      }
    } else {
      stdoutBuf += str;
      const lines = stdoutBuf.split('\n');
      stdoutBuf = lines.pop();
      for (const line of lines) {
        if (line.trim()) appendLog(containerName, line);
      }
    }
  };

  proc.stdout.on('data', (chunk) => flushLines(chunk, false));
  proc.stderr.on('data', (chunk) => flushLines(chunk, true));

  proc.on('close', () => {
    if (stdoutBuf.trim()) appendLog(containerName, stdoutBuf);
    if (stderrBuf.trim()) appendLog(containerName, stderrBuf);
    if (logStreamProcs.get(containerName) === proc) {
      logStreamProcs.delete(containerName);
    }
  });

  proc.on('error', () => {
    if (logStreamProcs.get(containerName) === proc) {
      logStreamProcs.delete(containerName);
    }
  });
}

function stopDockerLogStream(containerName) {
  const proc = logStreamProcs.get(containerName);
  if (proc) {
    try {
      proc.kill();
    } catch {
      // Abaikan
    }
    logStreamProcs.delete(containerName);
  }
}

function execFileAsync(cmd, args, timeoutMs = 4000) {
  return new Promise((resolve) => {
    execFile(
      cmd,
      args,
      { cwd: PROJECT_ROOT, timeout: timeoutMs, windowsHide: true },
      (err, stdout, stderr) => {
        resolve({ err, stdout: String(stdout || ''), stderr: String(stderr || '') });
      }
    );
  });
}

let isPollingDocker = false;

async function pollDockerStatus() {
  if (isPollingDocker) return;
  isPollingDocker = true;

  try {
    // Satu panggilan `docker inspect` untuk kedua container sekaligus (~3x lebih cepat di Windows)
    const inspectRes = await execFileAsync(
      'docker',
      [
        'inspect',
        '--format',
        '{{.Name}}|{{.State.Status}}|{{.State.Running}}|{{.State.StartedAt}}',
        ...CONSUMERS,
      ],
      6000
    );

    const errText = `${inspectRes.stderr} ${inspectRes.err ? inspectRes.err.message : ''}`;
    const daemonOffline =
      inspectRes.err &&
      (errText.includes('Cannot connect to the Docker daemon') ||
        errText.includes('error during connect') ||
        errText.includes('dockerDesktopLinuxEngine') ||
        errText.includes('ENOENT') ||
        errText.includes('ETIMEDOUT'));

    if (daemonOffline) {
      const wasAvailable = state.docker.available;
      state.docker.available = false;
      state.docker.error = 'Docker Engine belum berjalan (buka Docker Desktop).';
      for (const name of CONSUMERS) {
        stopDockerLogStream(name);
        state.docker.containers[name].running = false;
        state.docker.containers[name].status = 'docker-offline';
        state.docker.containers[name].connectedToBroker = false;
      }
      if (wasAvailable) {
        appendSystemLog('WARN', 'Docker Engine tidak terdeteksi / offline.');
      }
      broadcastSnapshot();
      return;
    }

    state.docker.available = true;
    state.docker.error = null;

    const inspectedMap = new Map();
    for (const rawLine of inspectRes.stdout.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line) continue;
      const parts = line.split('|');
      if (parts.length < 3) continue;
      const containerName = parts[0].replace(/^\//, '');
      inspectedMap.set(containerName, {
        status: parts[1] || 'unknown',
        running: parts[2] === 'true',
        startedAt: parts[3] || null,
      });
    }

    const runningNames = [];

    for (const name of CONSUMERS) {
      const prev = state.docker.containers[name];
      const info = inspectedMap.get(name);

      if (!info) {
        prev.exists = false;
        prev.running = false;
        prev.status = 'not-created';
        prev.connectedToBroker = false;
        prev.explicitlyDisconnected = false;
        stopDockerLogStream(name);
        continue;
      }

      const wasRunning = prev.running;
      prev.exists = true;
      prev.status = info.status;
      prev.running = info.running;
      prev.startedAt = info.startedAt;

      if (info.running) {
        runningNames.push(name);
        if (!logStreamProcs.has(name)) {
          attachDockerLogStream(name, info.startedAt);
        }
        if (!wasRunning && lastAttachedStartedAt.has(name)) {
          appendSystemLog('INFO', `Container ${name} aktif kembali (status: ${info.status}).`);
        }
      } else {
        prev.connectedToBroker = false;
        prev.explicitlyDisconnected = false;
        stopDockerLogStream(name);

        // Deteksi crash mendadak (mis. docker kill SIGKILL) saat ada pesan in-flight belum ACK
        if (wasRunning) {
          if (prev.inFlightMessageId) {
            const msgId = prev.inFlightMessageId;
            const msgEntry = state.messages[msgId];
            if (msgEntry && msgEntry.status === 'PROCESSING') {
              msgEntry.previousWorker = name;
              msgEntry.status = 'REQUEUED';
              msgEntry.updatedAt = timestamp();
            }
            prev.inFlightMessageId = null;
            appendSystemLog(
              'FAILOVER',
              `Container ${name} berhenti saat ${msgId} BELUM ACK! RabbitMQ mengembalikan pesan ke queue untuk diambil alih consumer aktif lain.`
            );
          } else {
            appendSystemLog('WARN', `Container ${name} berhenti (status: ${info.status}).`);
          }
        }
      }
    }

    // Sinkronisasi status koneksi jika log [READY] sudah tergeser melebihi --tail 80
    if (
      state.broker.online &&
      runningNames.length > 0 &&
      state.broker.consumerCount >= runningNames.length
    ) {
      for (const name of runningNames) {
        if (!state.docker.containers[name].explicitlyDisconnected) {
          state.docker.containers[name].connectedToBroker = true;
        }
      }
    } else if (!state.broker.online) {
      for (const name of runningNames) {
        state.docker.containers[name].connectedToBroker = false;
      }
    }

    broadcastSnapshot();
  } finally {
    isPollingDocker = false;
  }
}

// ==============================================================================
// 2. REAL-TIME RABBITMQ PASSIVE OBSERVER (TANPA MENJADI CONSUMER)
// ==============================================================================
let monitorConn = null;
let monitorChan = null;
let isPollingBroker = false;

async function ensureMonitorChannel() {
  if (monitorConn && monitorChan) return monitorChan;

  if (!monitorConn) {
    const conn = await amqp.connect(config.rabbitmqUrl, { timeout: 2500 });
    monitorConn = conn;

    conn.on('error', () => {
      monitorConn = null;
      monitorChan = null;
    });

    conn.on('close', () => {
      const wasOnline = state.broker.online;
      monitorConn = null;
      monitorChan = null;
      state.broker.online = false;
      state.broker.consumerCount = 0;
      state.broker.lastError = 'Koneksi ke RabbitMQ terputus (broker berhenti).';
      state.broker.checkedAt = timestamp();
      if (wasOnline) {
        appendSystemLog('DISCONNECTED', 'Broker RabbitMQ terdeteksi OFFLINE / berhenti!');
      }
      broadcastSnapshot();
    });
  }

  const ch = await monitorConn.createChannel();
  monitorChan = ch;
  ch.on('error', () => {
    monitorChan = null;
  });
  ch.on('close', () => {
    monitorChan = null;
  });
  return ch;
}

async function pollBrokerStatus() {
  if (isPollingBroker) return;
  isPollingBroker = true;

  try {
    const wasOnline = state.broker.online;
    const ch = await ensureMonitorChannel();

    try {
      // Passive check: hanya mengintip status queue, TIDAK membuat queue & TIDAK jadi consumer
      const q = await ch.checkQueue(config.queueName);
      state.broker.online = true;
      state.broker.queueExists = true;
      state.broker.messageCount = q.messageCount;
      state.broker.consumerCount = q.consumerCount;
      state.broker.lastError = null;
      state.broker.checkedAt = timestamp();
    } catch (checkErr) {
      const errMsg = String(checkErr && checkErr.message ? checkErr.message : checkErr);
      if (errMsg.includes('404') || errMsg.includes('NOT_FOUND')) {
        // Broker online, hanya queue belum dibuat oleh producer/consumer
        monitorChan = null;
        state.broker.online = true;
        state.broker.queueExists = false;
        state.broker.messageCount = 0;
        state.broker.consumerCount = 0;
        state.broker.lastError = null;
        state.broker.checkedAt = timestamp();
      } else {
        throw checkErr;
      }
    }

    if (!wasOnline && state.broker.online) {
      appendSystemLog('READY', `Broker RabbitMQ ONLINE (${config.maskedUrl}).`);
    }
  } catch (err) {
    const wasOnline = state.broker.online;
    monitorConn = null;
    monitorChan = null;
    state.broker.online = false;
    state.broker.consumerCount = 0;
    state.broker.lastError = diagnoseAmqpError(err);
    state.broker.checkedAt = timestamp();
    if (wasOnline) {
      appendSystemLog('DISCONNECTED', `RabbitMQ tidak dapat dijangkau: ${state.broker.lastError}`);
    }
  } finally {
    isPollingBroker = false;
    broadcastSnapshot();
  }
}

// ==============================================================================
// 3. REAL-TIME IDEMPOTENCY STORE SCANNER (./data/processed)
// ==============================================================================
function pollIdempotencyStore() {
  try {
    if (!fs.existsSync(config.idempotencyDir)) {
      state.idempotency = { count: 0, recent: [] };
      return;
    }

    const files = fs.readdirSync(config.idempotencyDir).filter((f) => f.endsWith('.json'));
    const records = [];

    for (const file of files) {
      try {
        const raw = fs.readFileSync(path.join(config.idempotencyDir, file), 'utf8');
        const parsed = JSON.parse(raw);
        records.push(parsed);
      } catch {
        // Abaikan file yang sedang ditulis
      }
    }

    records.sort((a, b) => String(b.processedAt || '').localeCompare(String(a.processedAt || '')));
    state.idempotency = {
      count: records.length,
      recent: records.slice(0, 15),
    };
  } catch {
    // Abaikan error sementara saat reset folder
  }
}

// ==============================================================================
// 4. REAL CLI ACTION EXECUTOR (WHITELISTED)
// ==============================================================================
let activeProducerProc = null;

function runProducerProcess(modeLabel, scriptArgs) {
  if (activeProducerProc) {
    return { ok: false, error: 'Producer sedang berjalan. Tunggu selesai atau hentikan dulu.' };
  }

  const proc = spawn('node', [path.join('src', 'producer.js'), ...scriptArgs], {
    cwd: PROJECT_ROOT,
    shell: false,
    windowsHide: true,
  });

  activeProducerProc = proc;
  state.producer = { running: true, mode: modeLabel, pid: proc.pid };
  appendSystemLog('INFO', `Menjalankan Producer (${modeLabel}): node src/producer.js ${scriptArgs.join(' ')}`.trim());
  broadcastSnapshot();

  let outBuf = '';
  let errBuf = '';

  const handleChunk = (chunk, isErr) => {
    const str = chunk.toString('utf8');
    if (isErr) {
      errBuf += str;
      const lines = errBuf.split('\n');
      errBuf = lines.pop();
      for (const line of lines) if (line.trim()) appendLog('producer', line);
    } else {
      outBuf += str;
      const lines = outBuf.split('\n');
      outBuf = lines.pop();
      for (const line of lines) if (line.trim()) appendLog('producer', line);
    }
  };

  proc.stdout.on('data', (c) => handleChunk(c, false));
  proc.stderr.on('data', (c) => handleChunk(c, true));

  proc.on('close', (code) => {
    if (outBuf.trim()) appendLog('producer', outBuf);
    if (errBuf.trim()) appendLog('producer', errBuf);
    activeProducerProc = null;
    state.producer = { running: false, mode: null, pid: null };
    if (code !== 0 && code !== null) {
      appendSystemLog('WARN', `Producer berakhir dengan exit code ${code}.`);
    }
    broadcastSnapshot();
  });

  proc.on('error', (err) => {
    activeProducerProc = null;
    state.producer = { running: false, mode: null, pid: null };
    appendSystemLog('ERROR', `Gagal menjalankan producer: ${err.message}`);
    broadcastSnapshot();
  });

  return { ok: true, message: `Producer (${modeLabel}) dijalankan.` };
}

function runOneShotCommand(label, cmd, args) {
  return new Promise((resolve) => {
    appendSystemLog('CMD', `${label}: ${cmd} ${args.join(' ')}`);
    const proc = spawn(cmd, args, {
      cwd: PROJECT_ROOT,
      shell: false,
      windowsHide: true,
    });

    let combined = '';
    proc.stdout.on('data', (d) => {
      combined += d.toString('utf8');
    });
    proc.stderr.on('data', (d) => {
      combined += d.toString('utf8');
    });

    proc.on('close', (code) => {
      const output = combined.trim();
      if (code === 0) {
        if (output) {
          for (const line of output.split('\n').slice(-4)) {
            if (line.trim()) appendSystemLog('INFO', line.trim());
          }
        }
        resolve({ ok: true, output });
      } else {
        const msg = output || `Perintah gagal dengan exit code ${code}`;
        appendSystemLog('ERROR', `${label} gagal: ${msg}`);
        resolve({ ok: false, error: msg });
      }
    });

    proc.on('error', (err) => {
      appendSystemLog('ERROR', `${label} gagal dijalankan: ${err.message}`);
      resolve({ ok: false, error: err.message });
    });
  });
}

async function handleAction(action) {
  switch (action) {
    case 'produce':
      return runProducerProcess('10 Pesan Normal', []);

    case 'produce-paced':
      return runProducerProcess('10 Pesan Paced 2s (Skenario 4-5)', ['--interval=2000']);

    case 'produce-duplicate':
      return runProducerProcess('Uji Idempotency (Batch Tetap)', ['--batch=DEMO-IDEMPOTENSI']);

    case 'produce-nack':
      return runProducerProcess('Uji NACK pada Pesan #3', ['--fail-once=3']);

    case 'stop-producer':
      if (!activeProducerProc) {
        return { ok: true, message: 'Tidak ada proses producer yang sedang berjalan.' };
      }
      try {
        activeProducerProc.kill();
        appendSystemLog('WARN', 'Proses Producer dihentikan secara manual dari Dashboard.');
        return { ok: true, message: 'Producer dihentikan.' };
      } catch (err) {
        return { ok: false, error: err.message };
      }

    case 'docker-up': {
      const res = await runOneShotCommand('Start Workers', 'docker', ['compose', 'up', '-d']);
      await pollDockerStatus();
      return res;
    }

    case 'docker-down': {
      const res = await runOneShotCommand('Stop All Workers', 'docker', ['compose', 'down']);
      await pollDockerStatus();
      return res;
    }

    case 'kill-c1': {
      const res = await runOneShotCommand('Kill Consumer 1 (SIGKILL)', 'docker', ['kill', 'consumer-1']);
      await pollDockerStatus();
      return res;
    }

    case 'stop-c1': {
      const res = await runOneShotCommand('Stop Consumer 1 (SIGTERM)', 'docker', ['stop', 'consumer-1']);
      await pollDockerStatus();
      return res;
    }

    case 'start-c1': {
      const res = await runOneShotCommand('Start Consumer 1', 'docker', ['compose', 'up', '-d', 'consumer-1']);
      await pollDockerStatus();
      return res;
    }

    case 'kill-c2': {
      const res = await runOneShotCommand('Kill Consumer 2 (SIGKILL)', 'docker', ['kill', 'consumer-2']);
      await pollDockerStatus();
      return res;
    }

    case 'start-c2': {
      const res = await runOneShotCommand('Start Consumer 2', 'docker', ['compose', 'up', '-d', 'consumer-2']);
      await pollDockerStatus();
      return res;
    }

    case 'reset-data': {
      const res = await runOneShotCommand('Reset Idempotency Data', 'node', [
        path.join('scripts', 'reset-data.js'),
      ]);
      state.messages = {};
      state.currentBatchId = null;
      for (const name of CONSUMERS) {
        state.docker.containers[name].ackedCount = 0;
        state.docker.containers[name].duplicateSkipCount = 0;
        state.docker.containers[name].nackCount = 0;
        state.docker.containers[name].hasFailover = false;
      }
      pollIdempotencyStore();
      broadcastSnapshot();
      return res;
    }

    case 'clear-view':
      state.logs['consumer-1'] = [];
      state.logs['consumer-2'] = [];
      state.logs.producer = [];
      state.messages = {};
      state.currentBatchId = null;
      broadcast('clear', {});
      broadcastSnapshot();
      return { ok: true, message: 'Tampilan log & tracker dibersihkan.' };

    default:
      return { ok: false, error: `Aksi tidak dikenal: ${action}` };
  }
}

// ==============================================================================
// 5. HTTP SERVER & SSE STREAMING
// ==============================================================================
function sendJson(res, statusCode, data) {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk.toString('utf8');
      if (raw.length > 32 * 1024) {
        reject(new Error('Body terlalu besar'));
      }
    });
    req.on('end', () => {
      if (!raw.trim()) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error('JSON tidak valid'));
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = reqUrl.pathname;

  if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    fs.readFile(htmlPath, 'utf8', (err, content) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Gagal membaca public/index.html');
        return;
      }
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      res.end(content);
    });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/state') {
    sendJson(res, 200, {
      state: getClientState(),
      logs: state.logs,
    });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
    });
    res.write(': connected\n\n');
    sseClients.add(res);

    // Kirim snapshot awal dan riwayat log yang sudah terkumpul
    res.write(`event: init\ndata: ${JSON.stringify({ state: getClientState(), logs: state.logs })}\n\n`);

    req.on('close', () => {
      sseClients.delete(res);
    });
    return;
  }

  if (req.method === 'POST' && pathname === '/api/action') {
    try {
      const body = await readJsonBody(req);
      const result = await handleAction(body.action);
      sendJson(res, result.ok ? 200 : 400, result);
    } catch (err) {
      sendJson(res, 400, { ok: false, error: err.message });
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ error: 'Not Found' }));
});

// ==============================================================================
// 6. LIFECYCLE & SHUTDOWN BERSIH
// ==============================================================================
const timers = [];

function shutdown() {
  console.log(`\n[${timestamp()}] [DASHBOARD] Menutup Web Monitoring Dashboard...`);
  for (const t of timers) clearInterval(t);
  for (const name of CONSUMERS) stopDockerLogStream(name);
  if (activeProducerProc) {
    try {
      activeProducerProc.kill();
    } catch {
      // Abaikan
    }
  }
  for (const res of sseClients) {
    try {
      res.end();
    } catch {
      // Abaikan
    }
  }
  if (monitorConn) {
    monitorConn.close().catch(() => {});
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, HOST, async () => {
  console.log('====================================================================');
  console.log(`[${timestamp()}] [DASHBOARD] Fault Tolerance Web Monitor aktif!`);
  console.log(`  URL        : http://${HOST}:${PORT}`);
  console.log(`  RabbitMQ   : ${config.maskedUrl} (queue: ${config.queueName})`);
  console.log(`  Mode Queue : ${config.singleActiveConsumer ? 'Single Active Consumer (Failover)' : 'Round-Robin'}`);
  console.log('  Catatan    : Flow CLI terminal tetap berjalan independen & sinkron.');
  console.log('====================================================================');

  pollIdempotencyStore();
  await Promise.all([pollBrokerStatus(), pollDockerStatus()]);

  timers.push(setInterval(pollBrokerStatus, 1500));
  timers.push(setInterval(pollDockerStatus, 2000));
  timers.push(setInterval(() => {
    pollIdempotencyStore();
    broadcastSnapshot();
  }, 2000));
  timers.push(setInterval(() => {
    for (const res of sseClients) {
      try {
        res.write(': ping\n\n');
      } catch {
        sseClients.delete(res);
      }
    }
  }, 15000));
});
