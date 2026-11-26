'use strict';

const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

function parseBool(value, defaultValue) {
  if (value === undefined || value === null || value === '') return defaultValue;
  return ['true', '1', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function parseIntSafe(value, defaultValue) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isNaN(parsed) ? defaultValue : parsed;
}

function formatVhostPath(vhost) {
  const cleaned = (vhost || '/').trim();
  if (cleaned === '' || cleaned === '/') {
    return '/';
  }
  if (cleaned.startsWith('/')) {
    return '/' + encodeURIComponent(cleaned);
  }
  return '/' + encodeURIComponent(cleaned);
}

function buildRabbitMqUrl() {
  const runningInDocker = parseBool(process.env.RUNNING_IN_DOCKER, false);
  const heartbeat = parseIntSafe(process.env.RABBITMQ_HEARTBEAT, 10);
  const rawUrl = (process.env.RABBITMQ_URL || '').trim();

  let parsedUrl;
  let source = 'components';

  if (rawUrl) {
    source = 'RABBITMQ_URL';
    parsedUrl = new URL(rawUrl);
  } else {
    const protocol = (process.env.RABBITMQ_PROTOCOL || 'amqp').trim();
    const host = (process.env.RABBITMQ_HOST || 'localhost').trim();
    const port = parseIntSafe(process.env.RABBITMQ_PORT, 5672);
    const user = (process.env.RABBITMQ_USER || 'guest').trim();
    const pass = process.env.RABBITMQ_PASS !== undefined ? process.env.RABBITMQ_PASS : 'guest';
    const vhostPath = formatVhostPath(process.env.RABBITMQ_VHOST || '/');

    const encodedUser = encodeURIComponent(user);
    const encodedPass = encodeURIComponent(pass);
    const constructed = `${protocol}://${encodedUser}:${encodedPass}@${host}:${port}${vhostPath}`;
    parsedUrl = new URL(constructed);
  }

  let hostTranslated = false;
  const originalHost = parsedUrl.hostname;
  if (
    runningInDocker &&
    ['localhost', '127.0.0.1', '::1', '[::1]'].includes(parsedUrl.hostname.toLowerCase())
  ) {
    parsedUrl.hostname = 'host.docker.internal';
    hostTranslated = true;
  }

  if (!parsedUrl.searchParams.has('heartbeat')) {
    parsedUrl.searchParams.set('heartbeat', String(heartbeat));
  }

  const maskedUrlObj = new URL(parsedUrl.toString());
  if (maskedUrlObj.password) {
    maskedUrlObj.password = '***';
  }

  return {
    rabbitmqUrl: parsedUrl.toString(),
    maskedUrl: maskedUrlObj.toString(),
    username: decodeURIComponent(parsedUrl.username || ''),
    runningInDocker,
    hostTranslated,
    originalHost,
    effectiveHost: parsedUrl.hostname,
    source,
    heartbeat,
  };
}

const urlInfo = buildRabbitMqUrl();

const config = {
  ...urlInfo,
  queueName: (process.env.QUEUE_NAME || 'krs_pemesanan_queue').trim(),
  singleActiveConsumer: parseBool(process.env.SINGLE_ACTIVE_CONSUMER, true),
  prefetchCount: Math.max(1, parseIntSafe(process.env.PREFETCH_COUNT, 1)),
  reconnectIntervalMs: Math.max(500, parseIntSafe(process.env.RECONNECT_INTERVAL_MS, 3000)),
  processDelayMs: Math.max(0, parseIntSafe(process.env.PROCESS_DELAY_MS, 4000)),
  preAckDelayMs: Math.max(0, parseIntSafe(process.env.PRE_ACK_DELAY_MS, 500)),
  messageCount: Math.max(1, parseIntSafe(process.env.MESSAGE_COUNT, 10)),
  startDelayMs: Math.max(0, parseIntSafe(process.env.START_DELAY_MS, 0)),
  idempotencyDir: path.resolve(
    __dirname,
    '..',
    (process.env.IDEMPOTENCY_DIR || './data/processed').trim()
  ),
};

function getQueueOptions() {
  const options = { durable: true };
  if (config.singleActiveConsumer) {
    options.arguments = {
      'x-single-active-consumer': true,
    };
  }
  return options;
}

function timestamp() {
  const now = new Date();
  const hh = String(now.getHours()).padStart(2, '0');
  const mm = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  const ms = String(now.getMilliseconds()).padStart(3, '0');
  return `${hh}:${mm}:${ss}.${ms}`;
}

function diagnoseAmqpError(err) {
  const msg = String(err && (err.message || err.code || err));

  if (msg.includes('ACCESS_REFUSED') || msg.includes('403')) {
    if (config.runningInDocker && config.username === 'guest') {
      return (
        'Login ditolak (ACCESS_REFUSED). User bawaan "guest" di RabbitMQ diblokir untuk koneksi ' +
        'non-loopback (termasuk dari Docker container ke Host via host.docker.internal). ' +
        'Buat user baru di RabbitMQ Host (mis. "admin") lalu perbarui RABBITMQ_USER dan RABBITMQ_PASS di file .env.'
      );
    }
    return 'Login ditolak (ACCESS_REFUSED). Periksa kembali RABBITMQ_USER, RABBITMQ_PASS, dan izin RABBITMQ_VHOST di file .env.';
  }

  if (msg.includes('PRECONDITION_FAILED') || msg.includes('406')) {
    return (
      `Konflik argumen queue "${config.queueName}" (406 PRECONDITION_FAILED). ` +
      'Queue dengan nama sama sudah ada di RabbitMQ dengan konfigurasi berbeda (misalnya durable atau x-single-active-consumer). ' +
      'Hapus queue lama melalui RabbitMQ Management UI / rabbitmqctl atau ganti QUEUE_NAME di .env.'
    );
  }

  if (msg.includes('ECONNREFUSED')) {
    return (
      `Koneksi ditolak ke ${config.maskedUrl} (ECONNREFUSED). ` +
      'Pastikan layanan RabbitMQ di Host sedang aktif dan listening pada port yang sesuai.'
    );
  }

  if (msg.includes('ENOTFOUND')) {
    return `Host RabbitMQ "${config.effectiveHost}" tidak ditemukan (ENOTFOUND). Periksa konfigurasi host/URL di .env.`;
  }

  return msg;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  config,
  getQueueOptions,
  timestamp,
  diagnoseAmqpError,
  sleep,
};
