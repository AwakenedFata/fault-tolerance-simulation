# Simulasi Fault Tolerance Sistem Terdistribusi (Node.js, RabbitMQ & Docker Worker)

Implementasi ini disusun untuk memenuhi seluruh persyaratan **Bagian 3.C (Implementasi Praktikum)**, **Bagian 4 (Pengujian Sistem & Catatan Teknis)**, dan **Bagian 8 (Pertanyaan Presentasi)** pada dokumen `Tugas_Fault_Tolerance_Sistem_Terdistribusi.pdf`, serta mengikuti arahan spesifik pada `Hint.jpeg`:

1. **Yang di-Docker-kan hanya Worker (Consumer), BUKAN RabbitMQ.** RabbitMQ dijalankan terpisah di Host OS (atau server eksternal).
2. **Hanya ada 1 file source code consumer (`src/consumer.js`)** dan 1 `Dockerfile` yang direplikasi menjadi 2 container worker (`consumer-1` dan `consumer-2`) di `docker-compose.yml`.
3. **Pola Failover Active-Passive (`x-single-active-consumer: true` + `prefetch(1)`)**: `consumer-1` menangani seluruh pesan terlebih dahulu; `consumer-2` berada dalam posisi *standby* di queue yang sama dan **otomatis mengambil alih** pesan yang belum di-ACK beserta seluruh sisa pesan begitu `consumer-1` dimatikan.
4. **Ketahanan Penuh Sesuai Catatan Teknis PDF**:
   - **Durable Queue** (`durable: true`)
   - **Persistent Message** (`persistent: true` / `deliveryMode: 2`)
   - **Publisher Confirms** (`createConfirmChannel()`) pada Producer
   - **Manual ACK & NACK** (`noAck: false`, `channel.ack`, `channel.nack`) pada Consumer
   - **Auto-Reconnect** pada Producer maupun Consumer saat RabbitMQ berhenti sementara (Skenario 4 & 5)
   - **Shared Atomic Idempotency Store** (`./data/processed/` via Docker volume + `fs.writeFileSync(..., { flag: 'wx' })`) untuk mencegah transaksi ganda jika terjadi pengiriman ulang (*redelivery*).

---

## Struktur Proyek

```text
.
├── .env                    # Konfigurasi kredensial RabbitMQ & parameter simulasi (edit di sini)
├── .env.example            # Template dokumentasi environment variables
├── Dockerfile              # Image Docker tunggal untuk worker (src/consumer.js)
├── docker-compose.yml      # Replikasi worker consumer-1 & consumer-2 (TANPA RabbitMQ)
├── package.json            # Dependency (amqplib, dotenv) dan shortcut script pengujian
├── scripts/
│   └── reset-data.js       # Membersihkan record idempotency sebelum pengujian baru
├── public/
│   └── index.html          # Tampilan Web Monitoring & Control Dashboard (opsional)
└── src/
    ├── config.js           # Loader .env, auto-translasi localhost -> host.docker.internal di Docker
    ├── idempotency.js      # Penyimpanan status idempotency atomik lintas-container
    ├── consumer.js         # 1 file consumer tunggal (direplikasi ke consumer-1 & consumer-2)
    ├── producer.js         # Producer 10 pesan KRS dengan Publisher Confirms & auto-reconnect
    └── dashboard.js        # Server observer Web UI ringan (SSE + docker logs + passive RMQ check)
```

---

## 1. Konfigurasi Kredensial RabbitMQ (Wajib Sebelum Menjalankan)

### Peringatan Penting: User `guest` pada RabbitMQ Lokal (Host Windows) vs Docker Worker

Secara default, RabbitMQ membatasi user bawaan `guest` **hanya untuk koneksi loopback (`127.0.0.1`)**. Karena `consumer-1` dan `consumer-2` berjalan di dalam Docker container dan mengakses RabbitMQ di Windows Host melalui `host.docker.internal`, koneksi tersebut sering dianggap non-loopback oleh RabbitMQ sehingga ditolak dengan error `ACCESS_REFUSED`.

**Langkah yang direkomendasikan di Host RabbitMQ (PowerShell / Command Prompt Administrator):**

```powershell
rabbitmqctl add_user admin admin123
rabbitmqctl set_user_tags admin administrator
rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
```

### Edit File `.env`

Buka file `.env` lalu masukkan kredensial yang telah Anda siapkan:

- **Jika RabbitMQ berjalan lokal di Host (Windows Service):**
  ```env
  RABBITMQ_PROTOCOL=amqp
  RABBITMQ_HOST=localhost
  RABBITMQ_PORT=5672
  RABBITMQ_USER=admin
  RABBITMQ_PASS=admin123
  RABBITMQ_VHOST=/
  RABBITMQ_URL=
  ```
  > Anda tidak perlu menulis `host.docker.internal` secara manual. `src/config.js` akan otomatis menggunakan `localhost` untuk `producer.js` di Host dan otomatis menerjemahkan `localhost` menjadi `host.docker.internal` untuk `consumer-1` dan `consumer-2` di dalam Docker.

- **Jika RabbitMQ berjalan di server remote / CloudAMQP:**
  Isi langsung `RABBITMQ_URL` (contoh: `amqps://user:pass@host.rmq.cloudamqp.com/vhost`), maka kedua sisi (Producer di Host & Worker di Docker) akan langsung memakai URL tersebut.

---

## 2. Instalasi & Menjalankan Sistem

1. Install dependency Node.js di Host (untuk menjalankan `producer.js`):
   ```powershell
   npm install
   ```

2. Jalankan 2 replika Docker Worker (`consumer-1` dan `consumer-2`):
   ```powershell
   npm run docker:up
   ```
   Tunggu beberapa detik hingga log menampilkan:
   - `[consumer-1] [READY] Terhubung ke RabbitMQ! Queue="krs_pemesanan_queue" (durable=true)`
   - `[consumer-2] [READY] Terhubung ke RabbitMQ! Queue="krs_pemesanan_queue" (durable=true)`

   *(Karena `consumer-1` memiliki `START_DELAY_MS=0` dan `consumer-2` memiliki `START_DELAY_MS=2500`, `consumer-1` selalu menjadi **Single Active Consumer** pertama dan `consumer-2` otomatis menjadi **Standby**).*

3. Buka terminal kedua di Host untuk menjalankan Producer:
   ```powershell
   npm run produce
   ```

4. **(Opsional) Menjalankan Versi Web UI Monitoring Dashboard**:
   Tanpa mengubah alur terminal di atas, Anda dapat membuka dashboard visual real-time di Host:
   ```powershell
   npm run ui
   ```
   Lalu buka `http://127.0.0.1:3000` di browser. Dashboard membaca stream asli `docker logs -f consumer-1` dan `consumer-2`, status passive queue RabbitMQ, folder `./data/processed`, serta tetap sinkron meskipun perintah `npm run produce` atau `npm run kill:c1` dijalankan langsung dari terminal CLI.

---

## 3. Panduan Lengkap 5 Skenario Pengujian (Bagian 4 PDF)

> Sebelum memulai ulang sesi uji agar data bersih, jalankan `npm run reset:data`.

### Skenario 1: Kedua Consumer Aktif
- **Langkah**:
  1. Jalankan `npm run docker:up` (pastikan `consumer-1` dan `consumer-2` aktif di `docker ps`).
  2. Di terminal lain, jalankan `npm run produce` (mengirim 10 pesan KRS).
- **Hasil yang Diamati**:
  - Dengan `SINGLE_ACTIVE_CONSUMER=true` (sesuai `Hint.jpeg`), `consumer-1` memproses pesan #1 sampai #10 secara berurutan dengan durasi 4 detik per pesan dan mengirim `ACK` setelah setiap pesan selesai, sementara `consumer-2` tetap aktif *standby* di queue yang sama.
  - Semua record tersimpan di folder `./data/processed/`.
  - *(Opsional jika ingin membandingkan mode Round-Robin Active-Active: ubah `SINGLE_ACTIVE_CONSUMER=false` di `.env`, hapus queue lama di RabbitMQ UI agar tidak `406 PRECONDITION_FAILED`, lalu restart worker).*

### Skenario 2 & 3: Consumer 1 Dimatikan Secara Paksa Sebelum ACK
- **Langkah**:
  1. Pastikan kedua container aktif (`npm run docker:up`).
  2. Jalankan `npm run produce` di terminal kedua.
  3. Amati log terminal pertama: `consumer-1` akan mulai memproses Pesan #1 (ACK), Pesan #2 (ACK), lalu masuk ke `[PROCESS] Memproses Pesan #3 ... (Status: UNACKED)`.
  4. **Tepat saat Pesan #3 sedang diproses (sebelum muncul log `[ACK]` untuk Pesan #3)**, jalankan di terminal kedua:
     ```powershell
     npm run kill:c1
     ```
     *(Perintah ini menjalankan `docker kill consumer-1`; perintah `npm run stop:c1` juga dirancang langsung keluar tanpa mengirim ACK jika sedang ada pesan in-flight).*
- **Hasil yang Diamati**:
  - `consumer-1` langsung mati (`Exited`). Pesan #3 **belum sempat di-ACK**.
  - RabbitMQ segera mendeteksi putusnya koneksi TCP `consumer-1`, mengembalikan Pesan #3 ke kepala queue dengan tanda `redelivered=true`, dan **seketika mempromosikan `consumer-2` menjadi Active Consumer**.
  - Log `consumer-2` langsung menampilkan:
    - `[RECEIVE] Menerima Pesan #3 (...) | redelivered=true`
    - `[FAILOVER] Pesan #3 (...) merupakan pesan REDELIVERED ... dan kini diambil alih oleh worker ini!`
  - `consumer-2` memproses Pesan #3 sampai #10 hingga seluruh 10 pesan selesai tanpa satu pun yang hilang.
- **Mengaktifkan kembali Consumer 1 setelah uji**:
  ```powershell
  npm run start:c1
  ```

### Skenario 4 & 5: RabbitMQ Dihentikan Sementara & Dijalankan Kembali
- **Langkah**:
  1. Pastikan kedua worker aktif (`npm run docker:up`).
  2. Jalankan Producer dalam mode bertahap (*paced*, jeda 2 detik per pesan) agar gangguan koneksi terlihat pada **Producer DAN Consumer sekaligus**:
     ```powershell
     npm run produce:paced
     ```
  3. Setelah 3–4 pesan terkirim, **hentikan layanan RabbitMQ di Host** (buka PowerShell sebagai Administrator):
     ```powershell
     Stop-Service RabbitMQ
     ```
     *(Atau `rabbitmqctl stop` sesuai cara instalasi RabbitMQ Anda).*
  4. **Amati Skenario 4**:
     - `consumer-1` dan `consumer-2` langsung mencetak `[DISCONNECTED] Koneksi ke RabbitMQ terputus` dan `[RECONNECT] Mencoba menghubungkan ulang dalam 3000ms...` (container tetap hidup, tidak crash).
     - `producer.js` langsung mendeteksi kegagalan pengiriman pada pesan berikutnya, berhenti sementara, dan melakukan *retry reconnect* setiap 3 detik tanpa kehilangan urutan pesan.
  5. Setelah ~10 detik, **jalankan kembali layanan RabbitMQ di Host**:
     ```powershell
     Start-Service RabbitMQ
     ```
  6. **Amati Skenario 5**:
     - Baik `producer.js` maupun kedua Docker worker (`consumer-1` & `consumer-2`) otomatis terhubung kembali (`[READY] Terhubung ke RabbitMQ!`).
     - `producer.js` langsung melanjutkan pengiriman dari pesan yang tertunda sampai pesan ke-10.
     - Queue `krs_pemesanan_queue` (`durable: true`) dan pesan yang sudah tersimpan (`persistent: true`) **tetap utuh** setelah RabbitMQ restart dan langsung diproses tuntas oleh worker.

### Uji Tambahan: Pembuktian Idempotency & NACK
- **Uji Idempotency (Anti Transaksi Ganda)**:
  1. Jalankan `npm run produce:duplicate` (mengirim batch `DEMO-IDEMPOTENSI` berisi 10 pesan dengan `messageId` tetap). Tunggu sampai semua selesai di-ACK.
  2. Jalankan `npm run produce:duplicate` **sekali lagi**.
  3. Amati log worker: seluruh 10 pesan terdeteksi sebagai duplikat (`[IDEMPOTENCY] ... SUDAH pernah diproses ... Melewati eksekusi ulang ... langsung mengirim ACK`).
- **Uji Mekanisme NACK (`requeue=true`)**:
  1. Jalankan `npm run produce:nack` (Pesan #3 akan mensimulasikan error sementara pada percobaan pertama).
  2. Amati log worker: Pesan #3 mendapat `[NACK] ... requeue=true`, kembali ke queue, lalu diterima ulang dengan `redelivered=true` dan berhasil di-ACK pada percobaan kedua.

---

## 4. Ringkasan Jawaban Pertanyaan Presentasi (Bagian 8 PDF)

1. **Apa yang terjadi jika kedua consumer mengalami kegagalan?**
   - Karena queue bersifat `durable: true` dan pesan bersifat `persistent: true`, pesan tidak akan hilang dan akan tetap mengantre (*Ready*) di dalam RabbitMQ (ditulis ke disk). Pesan yang sedang diproses sebelum kedua consumer mati (*Unacked*) otomatis dikembalikan ke status *Ready*. Begitu salah satu consumer dinyalakan kembali, pemrosesan langsung berlanjut. Risikonya adalah penumpukan antrean (*queue backlog*) dan keterlambatan layanan (*latency*) hingga worker pulih.

2. **Apakah RabbitMQ menjamin pesan tidak akan hilang? Jelaskan syarat dan batasannya.**
   - **Tidak otomatis 100% tanpa konfigurasi berlapis.** Syarat minimal agar pesan tahan terhadap restart/crash:
     1. **Durable Queue** (`durable: true`) agar definisi queue bertahan saat broker restart.
     2. **Persistent Message** (`persistent: true` / `deliveryMode: 2`) agar isi pesan ditulis ke disk.
     3. **Publisher Confirms** (`createConfirmChannel()`) agar Producer mendapat kepastian broker telah menyimpan pesan sebelum menganggap pengiriman sukses.
     4. **Manual ACK** (`noAck: false`) agar broker tidak menghapus pesan sebelum Consumer benar-benar tuntas memproses.
   - **Batasannya**:
     - Pada single-node RabbitMQ, terdapat jendela waktu singkat sebelum OS melakukan `fsync` penuh ke disk (jika listrik padam mendadak sebelum confirm/flush, pesan belum terkonfirmasi bisa hilang).
     - Jika disk fisik server RabbitMQ rusak permanen tanpa replikasi node (*single node SPOF*), seluruh data queue di disk ikut hilang. Solusi skala produksi adalah **RabbitMQ Cluster dengan Quorum Queues (konsensus Raft)** lintas beberapa node.

3. **Bagaimana mencegah pesan diproses dua kali?**
   - RabbitMQ pada pola *manual ACK* memberikan jaminan pengiriman **At-Least-Once Delivery** (bukan *Exactly-Once*), karena consumer bisa mati tepat setelah menyimpan data tetapi sebelum paket `ACK` sampai ke broker.
   - Solusinya adalah **Idempotent Consumer** (sudah diimplementasikan di `src/idempotency.js`): setiap pesan membawa `messageId` unik (`KRS-<batchId>-<seq>`). Sebelum memproses dan saat commit, worker memeriksa dan mencatat `messageId` tersebut secara atomik (`O_CREAT | O_EXCL` / pada produksi menggunakan kolom `UNIQUE(message_id)` di database bersama atau tabel `processed_messages` dalam satu transaksi DB). Jika pesan yang sama masuk ulang (`redelivered=true`), worker mendeteksi `messageId` sudah tercatat, melewati eksekusi bisnis, dan langsung mengirim `ACK`.

4. **Apakah sistem tetap berjalan jika database utama mati? Apa yang perlu ditambahkan?**
   - Producer tetap bisa menerima request dan menaruh pesan ke RabbitMQ (*decoupling / buffering*), tetapi Consumer akan gagal melakukan penyimpanan akhir ke database utama (pesan akan di-NACK/requeue atau tertahan di queue).
   - Yang perlu ditambahkan: **Database Replication (Primary–Replica/Standby)** dengan **Automatic Failover** (misalnya Patroni/Pgpool untuk PostgreSQL atau Orchestrator/Group Replication untuk MySQL), **Connection Pooling + Retry/Circuit Breaker**, **Dead Letter Queue (DLQ)** agar pesan gagal sementara tidak memblokir antrean utama, serta **Backup & Point-in-Time Recovery (PITR)**.

5. **Apa kelemahan arsitektur yang dibuat dan bagaimana rencana perbaikannya?**
   - **Kelemahan 1 (RabbitMQ Single Node = SPOF)**: Jika mesin Host tempat RabbitMQ berjalan rusak total, pengiriman dan konsumsi pesan terhenti. *Perbaikan*: Migrasi ke **RabbitMQ Cluster (minimal 3 node)** dengan **Quorum Queues**.
   - **Kelemahan 2 (Shared File Volume Idempotency)**: Penggunaan folder lokal `./data/processed` hanya berlaku jika container berada di host fisik yang sama. *Perbaikan*: Memindahkan *idempotency store* ke database terpusat/terdistribusi (PostgreSQL dengan `UNIQUE constraint` atau Redis Cluster).
   - **Kelemahan 3 (Belum ada Dead Letter Exchange / DLX & Retry Limit)**: Pesan yang gagal karena bug aplikasi berulang kali (`requeue=true`) dapat terus berputar di queue. *Perbaikan*: Menambahkan **Dead Letter Exchange (DLX)** + batas maksimal retry (*retry header*) dan *monitoring* (Prometheus + Grafana / RabbitMQ Management alerts).
   - **Kelemahan 4 (Active-Passive Throughput)**: Mode `x-single-active-consumer: true` memastikan urutan dan failover tunggal, tetapi hanya 1 worker yang aktif memproses pada satu waktu. *Perbaikan*: Untuk trafik tinggi (seperti puncak KRS ribuan mahasiswa) di mana urutan ketat per antrean global tidak wajib, gunakan mode **Competing Consumers (Active-Active)** dengan partisi per mahasiswa/fakultas atau auto-scaling worker di Kubernetes.
