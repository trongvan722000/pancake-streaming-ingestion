# Bài tập tổng ôn — toàn bộ repo từ đầu tới giờ

Làm theo thứ tự, đừng nhảy cóc — nhóm sau dựa trên hiểu đúng nhóm trước. Mỗi bài đều có bước tự kiểm chứng bằng lệnh thật, không chỉ đọc code là xong. Xong bài nào gửi kết quả/câu trả lời cho tao, tao chỉ chỗ sai trước khi qua bài tiếp.

## Nhóm A — Docker & kiến trúc service (làm trước tiên, vì vừa nhầm chỗ này)

**A1.** Chạy `docker compose config` — đọc output, chỉ ra: `gateway` và `consumer` build từ Dockerfile nào, khác nhau đúng ở dòng nào trong config sinh ra.

**A2.** Tự giải thích lại bằng lời của mày (không nhìn note): vì sao `python -m consumer.main` chạy trực tiếp trên máy Mac báo `ModuleNotFoundError: No module named 'confluent_kafka'`, nhưng `docker compose up consumer` thì không lỗi gì. 2 câu, không hơn.

**A3.** Thử nghiệm phá để hiểu rõ cơ chế: mở `docker-compose.yml`, tạm comment dòng `command: ["python", "-m", "consumer.main"]` ở service `consumer`, chạy `docker compose up -d --build consumer`, xem `docker logs consumer` — nó chạy nhầm code gì? Vì sao? Xong nhớ **bỏ comment lại** trước khi qua bài khác.

## Nhóm B — Kiến trúc Kafka lúc setup (listener, KRaft)

Dựa trên [kafka-listeners.md](kafka-listeners.md) — note này mày tự đúc kết từ trước, giờ làm bài tập để test lại có thật sự nhớ hay chỉ "đọc thấy quen mắt".

**B1.** 1 đoạn ngắn (không nhìn note): vì sao Kafka cần khái niệm "listener", trong khi 1 DB bình thường (Postgres...) connect 1 phát là xong, không cần khái niệm này?

**B2.** Mở `docker-compose.yml`, phần `kafka:` — chỉ đúng ra 3 dòng `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, giải thích **vai trò khác nhau** của từng dòng bằng lời của mày (không copy note). Gợi ý tự kiểm tra: nếu trả lời được cả 3 giống nhau hoặc na ná nhau, nghĩa là chưa phân biệt được, đọc lại note.

**B3.** Test address selection rule bằng tay, làm cả 2 lệnh, so sánh:
```bash
kcat -b localhost:9092 -L                         # từ HOST (Mac)
docker compose exec debug kafkacat -b kafka:9094 -L   # từ CONTAINER khác
```
Cả 2 đều phải chạy được — giải thích vì sao 2 lệnh phải dùng 2 địa chỉ bootstrap khác nhau để tới cùng 1 Kafka.

**B4.** Tái hiện lại đúng "bẫy" note đã cảnh báo (mục 5 & 6 trong note): từ container `debug`, thử gọi `kafkacat -b kafka:9092 -L` (cố tình dùng sai port — port này là listener `PLAINTEXT` dành cho host, không phải cho container). Lệnh `-L` có chạy được không? Có gì khác thường trong output (chú ý số `broker`)? Sau đó thử `-P` (produce) vào cùng địa chỉ sai đó — có khác gì so với `-L` không? Đây chính là lý do note nói "đừng tin `-L`".

**B5.** 1 câu: `KAFKA_PROCESS_ROLES: 'broker,controller'` nghĩa là gì, và vì sao setup này không cần thêm Zookeeper như các hướng dẫn Kafka đời cũ hay dùng?

**B6.** Áp dụng lại checklist mục 8 trong note cho chính service `consumer` mày mới thêm — tự chấm: `consumer` có dùng đúng `kafka:9094` (không phải `9092`) không? Có đọc `bootstrap.servers` qua biến môi trường (không hardcode) không? Đã test bằng produce/consume thật (không chỉ tin `-L`) chưa — dẫn chứng bằng chính lần test DLQ đã làm.

## Nhóm C — Webhook + FastAPI (`gateway/main.py`)

**C1.** Thử POST vào route GET (`/webhooks/pancake/{secret_token}` — đổi tạm decorator hoặc dùng `curl -X POST` vào URL đó khi route chỉ có `@app.get`). Ghi lại status code, giải thích vì sao khác 404 (token sai).

**C2.** `curl -X POST` với body không phải JSON hợp lệ (vd `curl -d "abc"`). Xem log gateway — payload lỗi này có bị mất không, hay được xử lý thế nào? Chỉ đúng dòng code xử lý case này.

## Nhóm D — Kafka Producer (`gateway/producer.py`)

**D1.** Tự vẽ lại (giấy hoặc text) đúng thứ tự chạy của `produce_event()` → `_producer.produce()` → background thread → `_delivery_report`. Khoanh tròn bước nào chạy đồng bộ (block code), bước nào chạy bất đồng bộ.

**D2.** POST 3 request cùng `conversation_id`, rồi 3 request khác nhau `conversation_id`. Dùng `kafka-console-consumer` với `--property print.partition=true` xác nhận đúng nhóm nào rơi cùng partition.

## Nhóm E — Kafka Consumer (`consumer/consumer.py`)

**E1.** 1 câu duy nhất: `Consumer.poll()` khác `Producer.poll()` ở điểm nào? (không được trả lời "cùng là poll" — phải nói rõ hành động thật sự khác nhau ra sao).

**E2.** Đọc lại `_flush_batch` — tại sao dòng `_consumer.commit(...)` xuất hiện ở **cả 2 nhánh** (thành công lẫn thất bại)? Nếu xoá dòng commit ở nhánh lỗi (nhánh DLQ) đi, hậu quả gì xảy ra?

**E3.** Test thật lại DLQ (đã làm 1 lần, làm lại để nhớ): `docker stop clickhouse`, POST vài request, xem log `attempt=1,2,3` rồi `sending to DLQ`. `docker start clickhouse` lại.

## Nhóm F — ClickHouse

**F1.** Vào `docker exec clickhouse clickhouse-client`, tự gõ tay: tạo 1 database test, 1 table `MergeTree` bất kỳ, insert vài dòng, `SELECT` lại. Không copy từ note, tự nhớ cú pháp.

**F2.** 1 câu: tại sao bảng `raw.pancake_raw` hiện dùng `MergeTree` chứ chưa dùng `ReplacingMergeTree`?

## Nhóm G — Cloudflare Tunnel

**G1.** 1 câu: vì sao Cloudflare Tunnel không phải là NAT port-forwarding, dù cả 2 đều giúp "người ngoài internet gọi được vào máy mình"?

**G2.** Phân biệt: quick tunnel dùng khi nào, named tunnel dùng khi nào — trong project này, cái nào đang dùng, cái nào cần cho việc đăng ký webhook thật với Pancake?

## Nhóm H — End-to-end (tổng hợp tất cả)

**H1.** Từ đầu tới cuối, không nhìn note: vẽ lại sơ đồ đầy đủ `Pancake → ... → ClickHouse`, ghi rõ tên từng service/container, port nào dùng ở đâu, cái gì chạy trong Docker, cái gì (nếu có) chạy ngoài host.

**H2.** Chạy full pipeline thật 1 lượt từ đầu: `docker compose up -d` toàn bộ stack → `curl POST` vào gateway → `docker logs -f gateway` thấy produce → `docker logs -f consumer` thấy insert → query ClickHouse thấy data → xoá sạch bằng `docker compose down`, làm lại từ đầu 1 lần nữa không cần xem note.

---

Đừng làm hết 1 lượt rồi mới báo — xong từng nhóm (A, B, C...) gửi câu trả lời/kết quả, tao xác nhận đúng/sai trước khi mày đi tiếp.
