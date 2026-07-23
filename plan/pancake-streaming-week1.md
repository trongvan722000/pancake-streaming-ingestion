# Plan: Pancake Streaming Ingestion (Week 1 scope)

## Context

Đây là project portfolio cá nhân, dựng lại kiến trúc "Conversation Intelligence Layer" mô tả trong RFC nhưng tự triển khai từ đầu (thư mục hiện đang hoàn toàn trống — chưa có code, docker-compose, dbt project, hay Airflow nào). Mục tiêu tuần này là hoàn thành riêng phần **streaming ingestion cho Pancake** (Haravan batch, dbt transform, AI enrichment sẽ làm sau).

Yêu cầu ban đầu: Pancake chỉ cho đăng ký **1 webhook URL duy nhất** (không tách theo loại event), và endpoint này sẽ được host "ở Cloudflare". Từ endpoint đó cần phân loại event (conversation vs message), đẩy vào đúng Kafka topic, mỗi topic có 1 consumer riêng ghi xuống ClickHouse.

Qua trao đổi, 2 quyết định đã chốt:
- **Consumer**: viết custom Python consumer service (không dùng ClickHouse Kafka Engine) — ưu tiên thể hiện kỹ năng engineering (validate, batch insert, retry, DLQ) cho CV.
- **Cloudflare endpoint**: mày chưa phân biệt rõ Worker vs Tunnel — bên dưới mình chốt giúp là **Cloudflare Tunnel** (giải thích rõ lý do), vì đơn giản hơn hẳn cho tuần này và vẫn thỏa đúng ràng buộc "endpoint ở Cloudflare".

## Các chỉnh sửa so với plan gốc của mày

1. **"Webhook là producer" — đúng, nhưng compute phải nằm ở FastAPI, không phải ở Cloudflare.**
   Cloudflare Worker chạy trên edge runtime (V8 isolate), không giữ được kết nối TCP bền tới Kafka broker theo cách ổn định như một Kafka client thư viện chuẩn (kafka-python/confluent-kafka) cần. Nếu cố làm Kafka producer ngay trên Worker, mày sẽ tốn cả tuần vật lộn với giao thức thay vì build pipeline.
   → Chỉ có **1 service duy nhất** đóng cả 2 vai trò webhook receiver + Kafka producer: **FastAPI gateway** chạy trong docker-compose. Cùng 1 request handler: nhận POST từ Pancake → verify → classify `type` → produce vào đúng Kafka topic bằng `confluent-kafka-python`. Không tách ra 2 service.
   - **Cloudflare Tunnel** chỉ là network plumbing (DNS + TLS) trỏ hostname public thẳng vào port FastAPI đang chạy local — không có code/logic gì ở lớp này, không cần viết Cloudflare Worker (JS) riêng, không cần mở port trên máy/VPS.
   (Nếu sau này muốn logic chạy thật sự ở edge — rate-limit, geo-block, verify chữ ký trước khi vào origin — lúc đó mới cần thêm Worker phía trước. Không cần cho MVP tuần này.)

2. **Phân loại event: làm ngay tại gateway, không cần topic "raw" trung gian.**
   Vì webhook payload của Pancake thường có sẵn field kiểu event (`type`/`event_name`...), gateway có thể route thẳng vào `pancake.conversations` hoặc `pancake.messages` mà không cần 1 topic raw + stream processor fan-out. Giữ 1 topic raw catch-all để replay là ý hay nhưng là việc của tuần sau (ghi chú lại, không làm ngay).

3. **Kafka: dùng bản KRaft (không cần Zookeeper).**
   `apache/kafka` official image hỗ trợ KRaft mode — 1 container, đỡ 1 service (Zookeeper) trong compose.

4. **Xác thực webhook**: cần 1 lớp bảo vệ endpoint (Pancake có thể không ký HMAC payload). Nếu không có signature verification từ Pancake, tối thiểu phải nhúng 1 secret token vào path/query khi đăng ký webhook URL (`.../webhooks/pancake/<random-token>`), gateway reject nếu token sai. Việc đầu tiên cần làm là đọc doc Pancake webhook để biết có HMAC signature không — quyết định lớp gateway sẽ ảnh hưởng bởi cái này.

5. **Đồng bộ schema raw layer với dbt models đã phác trong RFC.**
   RFC đã đặt tên `stg_pancake__conversations`, `stg_pancake__messages`. Đặt tên bảng raw ClickHouse khớp convention đó ngay từ đầu (database `raw`, table `pancake_conversations_raw` / `pancake_messages_raw`) để dbt source() sau này không phải sửa lại.

## Kiến trúc cuối cùng cho tuần này

```
Pancake (webhook) 
   -> Cloudflare Tunnel (public hostname, TLS, không mở port)
   -> FastAPI ingestion gateway (docker-compose)
        - verify secret/signature
        - validate payload tối thiểu (Pydantic)
        - classify type -> produce Kafka, key = conversation_id
   -> Kafka (KRaft, docker-compose)
        - topic: pancake.conversations (partitions=3)
        - topic: pancake.messages (partitions=3)
        - topic: pancake.conversations.dlq / pancake.messages.dlq
   -> Custom Python consumers (2 process, hoặc 1 app 2 thread)
        - confluent-kafka-python, consumer group riêng mỗi topic
        - Pydantic validate lại, batch insert (vd 100 msg hoặc 5s flush)
        - lỗi validate/insert liên tục -> đẩy sang DLQ topic
   -> ClickHouse
        - raw.pancake_conversations_raw (ReplacingMergeTree, order by conversation_id, version = event_ts)
        - raw.pancake_messages_raw (ReplacingMergeTree, order by message_id, version = event_ts)
        - lưu cả raw JSON gốc (String) + field trích xuất chính, để không mất dữ liệu nếu schema Pancake đổi
```

## Việc cần làm, theo milestone

**M1 — Khảo sát & hạ tầng nền (bắt buộc làm trước)**
- Đọc doc webhook của Pancake: field `type`/`event`, có HMAC signature không, timeout response bao lâu, có gửi test event được không.
- Trigger vài event thật/test để có sample payload thực tế (không đoán schema).
- Setup docker-compose: Kafka (KRaft), ClickHouse, Kafka UI (provectuslabs/kafka-ui — để debug/demo).
- Cài `cloudflared`, tạo tunnel trỏ về gateway (localhost lúc đầu để test trước khi có gateway thật).

**M2 — Ingestion gateway (FastAPI)**
- Endpoint `POST /webhooks/pancake/{secret_token}`.
- Verify token/signature, trả 200 nhanh.
- Pydantic model cho envelope (loose/optional fields vì chưa chắc schema đầy đủ).
- Classify theo `type`, produce Kafka (key=conversation_id, acks=all).
- Log structured (request id, event type, kafka offset) để trace được từng event.

**M3 — Kafka topics + consumers**
- Tạo topics (script hoặc `kafka-topics.sh` trong compose init).
- Consumer app: đọc, validate, batch insert ClickHouse, commit offset sau khi insert thành công (at-least-once).
- DLQ: retry N lần rồi đẩy message lỗi + lý do lỗi vào topic `.dlq`.

**M4 — ClickHouse raw tables**
- Tạo database `raw`, 2 bảng ReplacingMergeTree như trên.
- Cân nhắc thêm cột `_kafka_partition`, `_kafka_offset`, `_ingested_at` để trace lineage.

**M5 — Đăng ký webhook thật & test end-to-end**
- Đăng ký URL tunnel vào Pancake dashboard.
- Gửi event thật, xác nhận đi hết pipeline: Pancake -> tunnel -> gateway -> Kafka (thấy trong Kafka UI) -> ClickHouse có row.
- Test idempotency: replay cùng 1 event 2 lần, confirm ReplacingMergeTree dedup đúng (`SELECT ... FINAL`).
- Test lỗi: gửi payload sai schema, confirm rơi vào DLQ thay vì crash consumer.
- (Nếu kịp) README + sơ đồ kiến trúc — dùng luôn cho phần portfolio/CV.

## Việc cần xác nhận trước khi code (chưa biết, phải tra doc Pancake)
- ~~Pancake webhook payload thật sự có field phân biệt loại event không, tên field là gì.~~
  **Đã xác nhận (đọc doc chính thức `docs.pancake.biz/pancake/st-f12/st-p2`):** có 5 loại event cấp cao: `messaging`, `conversation`, `subscription`, `post`, `connect_status`. Chưa biết tên field JSON chứa giá trị này (doc không show payload mẫu) — cần sample payload thật để map chính xác.
- ~~Pancake có ký HMAC signature cho webhook không, hay chỉ dựa vào secret trong URL.~~
  **Đã xác nhận: KHÔNG có HMAC.** Doc không hề nhắc tới ký payload — cách hiện tại (secret token nhúng trong URL path) là đúng hướng, không cần thêm verify signature.
- ~~Response timeout Pancake yêu cầu là bao nhiêu giây~~
  **Đã xác nhận: khuyến nghị trả HTTP 200 dưới 5 giây**, xử lý phần nặng bất đồng bộ (queue/background job) — khớp với cách gateway hiện tại đang làm (trả 200 ngay sau khi `produce()` bỏ vào buffer, không đợi Kafka ACK).

## Ràng buộc vận hành khác (mới đọc được từ doc, chưa có trong bản gốc)
- **Đăng ký Webhook không tự-service 100%**: phải gửi `page_id`/URL page cho đội support Pancake để họ bật tính năng, và mỗi page bật Webhook tốn 1 connection slot trong subscription — cần check Subscription settings còn slot trống trước khi cấu hình URL trong page settings.
- **Tự động suspend webhook** nếu trong 30 phút: error rate > 80% VÀ số request lỗi ≥ 300 (lỗi = status ngoài 2xx, timeout, hoặc network error). Suspend rồi phải tự vào Webhook Settings bật lại tay — không tự phục hồi.
- **Có thể gửi trùng event** (duplicate delivery) — xác nhận thêm lý do dùng `ReplacingMergeTree` ở ClickHouse (mục M4) là đúng, cần xử lý idempotent ở cả consumer.
