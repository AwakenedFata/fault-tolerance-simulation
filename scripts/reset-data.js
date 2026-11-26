'use strict';

const fs = require('fs');
const path = require('path');
const { config } = require('../src/config');

const dir = config.idempotencyDir;

if (!fs.existsSync(dir)) {
  console.log(`[RESET] Direktori idempotency belum ada (${dir}). Tidak ada yang perlu dihapus.`);
  process.exit(0);
}

const files = fs.readdirSync(dir).filter((file) => file.endsWith('.json'));
for (const file of files) {
  fs.unlinkSync(path.join(dir, file));
}

console.log(`[RESET] Berhasil menghapus ${files.length} file record idempotency dari: ${dir}`);
