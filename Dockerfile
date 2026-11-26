FROM node:24-alpine

WORKDIR /app

# Install dependency terlebih dahulu untuk memanfaatkan layer cache Docker
COPY package*.json ./
RUN npm ci --omit=dev || npm install --omit=dev

# Salin source code (1 file consumer yang sama untuk semua replica worker)
COPY src ./src

# Pastikan direktori data untuk shared idempotency store tersedia
RUN mkdir -p /app/data/processed

ENV NODE_ENV=production
ENV RUNNING_IN_DOCKER=true
ENV IDEMPOTENCY_DIR=/app/data/processed

# Menjalankan 1 file consumer yang sama di setiap container replika
CMD ["node", "src/consumer.js"]
