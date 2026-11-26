'use strict';

const fs = require('fs');
const path = require('path');
const { config } = require('./config');

function sanitizeKey(messageId) {
  return String(messageId).replace(/[^a-zA-Z0-9._-]/g, '_');
}

function ensureStoreDir() {
  fs.mkdirSync(config.idempotencyDir, { recursive: true });
}

function getRecordPath(messageId) {
  return path.join(config.idempotencyDir, `${sanitizeKey(messageId)}.json`);
}

function getProcessedRecord(messageId) {
  ensureStoreDir();
  const filePath = getRecordPath(messageId);
  if (!fs.existsSync(filePath)) {
    return null;
  }

  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return { messageId, processedBy: 'unknown', processedAt: 'unknown' };
  }
}

/**
 * Menyimpan penanda idempotency secara atomik menggunakan flag 'wx' (O_CREAT | O_EXCL).
 * Jika dua worker mencoba memproses messageId yang sama secara bersamaan,
 * hanya satu operasi tulis yang akan berhasil dan sisanya otomatis mendeteksi duplikasi.
 */
function markAsProcessed(messageId, metadata) {
  ensureStoreDir();
  const filePath = getRecordPath(messageId);
  const record = {
    messageId,
    ...metadata,
  };

  try {
    fs.writeFileSync(filePath, JSON.stringify(record, null, 2), {
      encoding: 'utf8',
      flag: 'wx',
    });
    return { created: true, record };
  } catch (err) {
    if (err && err.code === 'EEXIST') {
      const existing = getProcessedRecord(messageId);
      return { created: false, existing };
    }
    throw err;
  }
}

module.exports = {
  ensureStoreDir,
  getProcessedRecord,
  markAsProcessed,
};
