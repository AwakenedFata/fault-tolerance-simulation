'use strict';

const amqp = require('amqplib');
const { config, getQueueOptions, timestamp, diagnoseAmqpError, sleep } = require('./config');

const MATA_KULIAH_LIST = [
  { nama: 'Sistem Terdistribusi (IF501)', sks: 3 },
  { nama: 'Jaringan Komputer Lanjut (IF502)', sks: 3 },
  { nama: 'Keamanan Informasi (IF503)', sks: 3 },
  { nama: 'Pemrograman Web Lanjut (IF504)', sks: 4 },
  { nama: 'Rekayasa Perangkat Lunak (IF505)', sks: 3 },
];

function parseArgs(argv) {
  const args = {
    count: config.messageCount,
    intervalMs: 0,
    batchId: null,
    failOnceOrder: null,
  };

  for (const raw of argv) {
    if (raw.startsWith('--count=')) {
      const val = Number.parseInt(raw.split('=')[1], 10);
      if (!Number.isNaN(val) && val > 0) args.count = val;
    } else if (raw.startsWith('--interval=')) {
      const val = Number.parseInt(raw.split('=')[1], 10);
      if (!Number.isNaN(val) && val >= 0) args.intervalMs = val;
    } else if (raw.startsWith('--batch=')) {
      const val = raw.split('=')[1].trim();
      if (val) args.batchId = val;
    } else if (raw.startsWith('--fail-once=')) {
      const val = Number.parseInt(raw.split('=')[1], 10);
      if (!Number.isNaN(val) && val > 0) args.failOnceOrder = val;
    }
  }

  if (!args.batchId) {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    args.batchId =
      `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-` +
      `${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
  }

  return args;
}

function log(level, message) {
  console.log(`[${timestamp()}] [PRODUCER] [${level}] ${message}`);
}

function buildOrderMessages(args) {
  const messages = [];
  for (let i = 1; i <= args.count; i += 1) {
    const seq = String(i).padStart(3, '0');
    const mk = MATA_KULIAH_LIST[(i - 1) % MATA_KULIAH_LIST.length];
    const messageId = `KRS-${args.batchId}-${seq}`;

    messages.push({
      messageId,
      orderNumber: i,
      batchId: args.batchId,
      nim: `2301${String(1000 + i)}`,
      namaMahasiswa: `Mahasiswa ${i}`,
      mataKuliah: mk.nama,
      sks: mk.sks,
      semester: 'Ganjil 2026/2027',
      simulateFailureOnce: args.failOnceOrder === i,
      createdAt: new Date().toISOString(),
    });
  }
  return messages;
}

async function createBrokerSession() {
  while (true) {
    try {
      log('CONNECT', `Menghubungkan ke RabbitMQ di ${config.maskedUrl}...`);
      const connection = await amqp.connect(config.rabbitmqUrl);
      let alive = true;

      connection.on('error', (err) => {
        alive = false;
        log('DISCONNECTED', `Error koneksi RabbitMQ: ${diagnoseAmqpError(err)}`);
      });

      connection.on('close', () => {
        alive = false;
        log('DISCONNECTED', 'Koneksi RabbitMQ terputus.');
      });

      // Menggunakan ConfirmChannel (Publisher Confirms) agar producer memastikan
      // pesan benar-benar sudah diterima & ditulis oleh broker RabbitMQ.
      const channel = await connection.createConfirmChannel();
      channel.on('error', (err) => {
        alive = false;
        log('ERROR', `Error pada ConfirmChannel: ${diagnoseAmqpError(err)}`);
      });

      channel.on('close', () => {
        alive = false;
      });

      await channel.assertQueue(config.queueName, getQueueOptions());
      log(
        'READY',
        `Terhubung! Queue="${config.queueName}" (durable=true, ` +
          `singleActiveConsumer=${config.singleActiveConsumer})`
      );

      return {
        connection,
        channel,
        isAlive: () => alive,
      };
    } catch (err) {
      log(
        'DISCONNECTED',
        `Gagal terhubung ke RabbitMQ: ${diagnoseAmqpError(err)} | ` +
          `Mencoba reconnect dalam ${config.reconnectIntervalMs}ms...`
      );
      await sleep(config.reconnectIntervalMs);
    }
  }
}

function publishWithConfirm(channel, queueName, payload) {
  return new Promise((resolve, reject) => {
    const buffer = Buffer.from(JSON.stringify(payload), 'utf8');
    const options = {
      persistent: true, // deliveryMode: 2 (pesan ditulis ke disk pada durable queue)
      contentType: 'application/json',
      messageId: payload.messageId,
      timestamp: Math.floor(Date.now() / 1000),
    };

    try {
      channel.sendToQueue(queueName, buffer, options, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    } catch (err) {
      reject(err);
    }
  });
}

async function closeSessionQuietly(session) {
  if (!session) return;
  try {
    if (session.channel) await session.channel.close();
  } catch {
    // Abaikan jika sudah tertutup
  }
  try {
    if (session.connection) await session.connection.close();
  } catch {
    // Abaikan jika sudah tertutup
  }
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const messages = buildOrderMessages(args);

  log(
    'START',
    `Menyiapkan ${messages.length} pesan pemesanan KRS (batch="${args.batchId}", ` +
      `interval=${args.intervalMs}ms, persistent=true, publisherConfirm=true)`
  );

  if (args.failOnceOrder) {
    log(
      'INFO',
      `Mode uji NACK aktif: Pesan #${args.failOnceOrder} akan mensimulasikan kegagalan sementara ` +
        'pada percobaan pertama (NACK + requeue=true).'
    );
  }

  let session = await createBrokerSession();
  let index = 0;

  while (index < messages.length) {
    const payload = messages[index];

    if (!session || !session.isAlive()) {
      log(
        'RECONNECT',
        `Koneksi terputus sebelum Pesan #${payload.orderNumber} (${payload.messageId}). ` +
          'Menghubungkan ulang ke RabbitMQ...'
      );
      await closeSessionQuietly(session);
      session = await createBrokerSession();
    }

    try {
      await publishWithConfirm(session.channel, config.queueName, payload);
      log(
        'SENT',
        `[${payload.orderNumber}/${messages.length}] Terkirim & Terkonfirmasi (Broker ACK): ` +
          `${payload.messageId} | NIM=${payload.nim} | MK="${payload.mataKuliah}"`
      );
      index += 1;

      if (args.intervalMs > 0 && index < messages.length) {
        await sleep(args.intervalMs);
      }
    } catch (err) {
      log(
        'WARN',
        `Gagal mengirim Pesan #${payload.orderNumber} (${payload.messageId}): ${diagnoseAmqpError(err)}. ` +
          'Menunggu RabbitMQ kembali aktif untuk melanjutkan dari pesan yang sama...'
      );
      await closeSessionQuietly(session);
      session = null;
      await sleep(config.reconnectIntervalMs);
    }
  }

  log(
    'DONE',
    `Seluruh ${messages.length} pesan pada batch "${args.batchId}" berhasil dikirim dan dikonfirmasi RabbitMQ.`
  );
  await closeSessionQuietly(session);
}

run().catch((err) => {
  log('FATAL', err.message || String(err));
  process.exit(1);
});
