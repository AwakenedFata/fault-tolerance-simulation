'use strict';

const os = require('os');
const amqp = require('amqplib');
const { config, getQueueOptions, timestamp, diagnoseAmqpError, sleep } = require('./config');
const idempotency = require('./idempotency');

const WORKER_ID = (process.env.WORKER_ID || os.hostname()).trim();

let connection = null;
let channel = null;
let reconnectTimer = null;
let isShuttingDown = false;
let isConnectionAlive = false;
let inFlightMessage = null;
let stats = {
  acked: 0,
  nackedRequeued: 0,
  nackedDiscarded: 0,
  skippedDuplicate: 0,
};

function log(level, message) {
  console.log(`[${timestamp()}] [${WORKER_ID}] [${level}] ${message}`);
}

function safeAck(ch, msg) {
  if (!isConnectionAlive || !ch) return false;
  try {
    ch.ack(msg);
    return true;
  } catch (err) {
    log('WARN', `Gagal mengirim ACK (channel sudah tertutup): ${err.message}`);
    return false;
  }
}

function safeNack(ch, msg, requeue) {
  if (!isConnectionAlive || !ch) return false;
  try {
    ch.nack(msg, false, requeue);
    return true;
  } catch (err) {
    log('WARN', `Gagal mengirim NACK (channel sudah tertutup): ${err.message}`);
    return false;
  }
}

function scheduleReconnect(reason) {
  if (isShuttingDown) return;

  isConnectionAlive = false;
  channel = null;
  connection = null;

  if (reconnectTimer) return;

  log(
    'RECONNECT',
    `${reason}. Mencoba menghubungkan ulang ke RabbitMQ dalam ${config.reconnectIntervalMs}ms...`
  );

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectAndConsume();
  }, config.reconnectIntervalMs);
}

async function handleMessage(activeChannel, msg) {
  if (msg === null) {
    log('WARN', 'Consumer dibatalkan oleh RabbitMQ (queue dihapus atau broker shutdown).');
    scheduleReconnect('Consumer dibatalkan oleh broker');
    return;
  }

  const redelivered = Boolean(msg.fields.redelivered);
  const deliveryTag = msg.fields.deliveryTag;

  let payload;
  try {
    payload = JSON.parse(msg.content.toString('utf8'));
  } catch (parseErr) {
    stats.nackedDiscarded += 1;
    log(
      'NACK',
      `Format JSON rusak pada deliveryTag=${deliveryTag} (${parseErr.message}). ` +
        'Mengirim NACK (requeue=false) agar tidak menyebabkan infinite loop.'
    );
    safeNack(activeChannel, msg, false);
    return;
  }

  const messageId = payload.messageId || `delivery-${deliveryTag}`;
  const orderNumber = payload.orderNumber ?? '?';
  const nim = payload.nim || '-';
  const mataKuliah = payload.mataKuliah || '-';

  inFlightMessage = { messageId, orderNumber, deliveryTag };

  try {
    // 1. Pengecekan Idempotency (Mencegah pemrosesan/transaksi ganda saat redelivery)
    const existingRecord = idempotency.getProcessedRecord(messageId);
    if (existingRecord) {
      stats.skippedDuplicate += 1;
      log(
        'IDEMPOTENCY',
        `Pesan #${orderNumber} (${messageId}) [redelivered=${redelivered}] SUDAH pernah diproses ` +
          `oleh "${existingRecord.processedBy}" pada ${existingRecord.processedAt}. ` +
          'Melewati eksekusi ulang untuk mencegah transaksi ganda -> langsung mengirim ACK.'
      );
      safeAck(activeChannel, msg);
      return;
    }

    log(
      'RECEIVE',
      `Menerima Pesan #${orderNumber} (${messageId}) | NIM: ${nim} | MK: ${mataKuliah} | ` +
        `redelivered=${redelivered} | deliveryTag=${deliveryTag}`
    );

    if (redelivered) {
      log(
        'FAILOVER',
        `Pesan #${orderNumber} (${messageId}) merupakan pesan REDELIVERED (dikembalikan ke queue ` +
          'dari kegagalan/NACK sebelumnya) dan kini diambil alih oleh worker ini!'
      );
    }

    // 2. Simulasi pemrosesan transaksi KRS (belum di-ACK)
    log(
      'PROCESS',
      `Memproses Pesan #${orderNumber} (${messageId}) selama ${config.processDelayMs}ms ` +
        '(Status: UNACKED — matikan container sekarang untuk menguji Skenario 3)...'
    );
    await sleep(config.processDelayMs);

    // Jika koneksi RabbitMQ terputus saat proses masih berjalan, batalkan commit & ACK
    if (!isConnectionAlive || channel !== activeChannel) {
      log(
        'WARN',
        `Koneksi RabbitMQ terputus saat Pesan #${orderNumber} (${messageId}) sedang diproses. ` +
          'Membatalkan commit lokal dan menunggu pengiriman ulang setelah reconnect.'
      );
      return;
    }

    // 3. Simulasi NACK (jika payload diberi flag simulateFailureOnce dan ini percobaan pertama)
    if (payload.simulateFailureOnce === true && !redelivered) {
      stats.nackedRequeued += 1;
      log(
        'NACK',
        `Simulasi kegagalan sementara pada Pesan #${orderNumber} (${messageId}). ` +
          'Mengirim NACK (requeue=true) agar pesan dikembalikan ke queue untuk dicoba ulang.'
      );
      safeNack(activeChannel, msg, true);
      return;
    }

    // 4. Simpan penanda idempotency secara atomik (mensimulasikan commit transaksi ke DB bersama)
    const commitResult = idempotency.markAsProcessed(messageId, {
      orderNumber,
      batchId: payload.batchId || '-',
      nim,
      namaMahasiswa: payload.namaMahasiswa || '-',
      mataKuliah,
      sks: payload.sks || 0,
      processedBy: WORKER_ID,
      processedAt: new Date().toISOString(),
      redelivered,
    });

    if (!commitResult.created) {
      stats.skippedDuplicate += 1;
      log(
        'IDEMPOTENCY',
        `Duplikasi terdeteksi saat commit atomik untuk Pesan #${orderNumber} (${messageId}). ` +
          `Sudah dikomit oleh "${commitResult.existing.processedBy}". Mengirim ACK tanpa transaksi ganda.`
      );
      safeAck(activeChannel, msg);
      return;
    }

    // Jeda singkat antara commit dan ACK (menggambarkan window crash pasca-commit pra-ACK)
    if (config.preAckDelayMs > 0) {
      await sleep(config.preAckDelayMs);
    }

    // 5. Kirim Manual ACK setelah pemrosesan benar-benar tuntas
    if (safeAck(activeChannel, msg)) {
      stats.acked += 1;
      log(
        'ACK',
        `Pesan #${orderNumber} (${messageId}) BERHASIL diproses dan di-ACK ` +
          `(total ACK=${stats.acked}, duplikat di-skip=${stats.skippedDuplicate}).`
      );
    }
  } catch (err) {
    stats.nackedRequeued += 1;
    log(
      'ERROR',
      `Terjadi error saat memproses Pesan #${orderNumber} (${messageId}): ${err.message}. ` +
        'Mengirim NACK (requeue=true).'
    );
    safeNack(activeChannel, msg, true);
  } finally {
    inFlightMessage = null;
  }
}

async function connectAndConsume() {
  if (isShuttingDown) return;

  try {
    log('CONNECT', `Menghubungkan ke RabbitMQ di ${config.maskedUrl}...`);
    const conn = await amqp.connect(config.rabbitmqUrl);
    connection = conn;

    conn.on('error', (err) => {
      if (isShuttingDown) return;
      log('DISCONNECTED', `Error koneksi RabbitMQ: ${diagnoseAmqpError(err)}`);
    });

    conn.on('close', () => {
      if (isShuttingDown) return;
      log('DISCONNECTED', 'Koneksi ke RabbitMQ terputus (broker berhenti atau gangguan jaringan).');
      scheduleReconnect('Koneksi tertutup');
    });

    const ch = await conn.createChannel();
    channel = ch;

    ch.on('error', (err) => {
      if (isShuttingDown) return;
      log('ERROR', `Error pada AMQP channel: ${diagnoseAmqpError(err)}`);
    });

    ch.on('close', () => {
      if (isShuttingDown) return;
      isConnectionAlive = false;
      log('WARN', 'AMQP channel ditutup.');
    });

    const queueOptions = getQueueOptions();
    await ch.assertQueue(config.queueName, queueOptions);
    await ch.prefetch(config.prefetchCount);

    isConnectionAlive = true;

    const modeLabel = config.singleActiveConsumer
      ? 'SINGLE ACTIVE CONSUMER (Active-Passive Failover)'
      : 'ROUND-ROBIN (Active-Active Competing Consumers)';

    log('READY', `Terhubung ke RabbitMQ! Queue="${config.queueName}" (durable=true)`);
    log(
      'CONFIG',
      `Mode=${modeLabel} | prefetch=${config.prefetchCount} | manualACK=true | ` +
        `processDelay=${config.processDelayMs}ms | idempotencyDir=${config.idempotencyDir}`
    );

    if (config.singleActiveConsumer) {
      log(
        'STANDBY',
        'Menunggu pesan... (Jika worker ini Active Consumer, seluruh pesan akan masuk ke sini; ' +
          'jika Standby, worker ini diam menunggu hingga Active Consumer mati lalu langsung mengambil alih).'
      );
    } else {
      log('WAITING', 'Menunggu pesan dari queue...');
    }

    await ch.consume(
      config.queueName,
      (msg) => {
        handleMessage(ch, msg);
      },
      { noAck: false }
    );
  } catch (err) {
    log('ERROR', `Gagal inisialisasi consumer: ${diagnoseAmqpError(err)}`);
    scheduleReconnect('Gagal terhubung ke RabbitMQ');
  }
}

async function handleShutdownSignal(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  // Jika sedang memproses pesan (BELUM ACK), keluar seketika tanpa ACK
  // agar baik `docker kill` maupun `docker stop` konsisten mensimulasikan Skenario 3.
  if (inFlightMessage) {
    log(
      'CRASH-SIM',
      `Menerima sinyal ${signal} saat sedang memproses Pesan #${inFlightMessage.orderNumber} ` +
        `(${inFlightMessage.messageId}) SEBELUM ACK! Menghentikan proses secara paksa tanpa ACK ` +
        'agar RabbitMQ mengembalikan pesan ke queue untuk diambil alih consumer lain.'
    );
    process.exit(1);
  }

  log('SHUTDOWN', `Menerima sinyal ${signal} dalam kondisi idle. Menutup koneksi RabbitMQ...`);
  try {
    if (channel) await channel.close();
    if (connection) await connection.close();
  } catch {
    // Abaikan error penutupan koneksi saat shutdown
  }
  process.exit(0);
}

process.on('SIGTERM', () => handleShutdownSignal('SIGTERM'));
process.on('SIGINT', () => handleShutdownSignal('SIGINT'));

async function start() {
  idempotency.ensureStoreDir();

  log('BOOT', `Memulai worker "${WORKER_ID}" (1 file consumer: src/consumer.js)`);
  if (config.hostTranslated) {
    log(
      'CONFIG',
      `Deteksi lingkungan Docker: menerjemahkan host "${config.originalHost}" -> "${config.effectiveHost}"`
    );
  }

  if (config.startDelayMs > 0) {
    log(
      'BOOT',
      `Menunggu jeda startup ${config.startDelayMs}ms agar consumer utama mendaftar terlebih dahulu...`
    );
    await sleep(config.startDelayMs);
  }

  await connectAndConsume();
}

start();
