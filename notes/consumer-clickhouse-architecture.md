# Consumer + ClickHouse — Lý thuyết & Setup

Note này nối tiếp [webhook-fastapi-kafka-producer.md](webhook-fastapi-kafka-producer.md) (đã có: webhook, FastAPI, Kafka producer, Cloudflare Tunnel) — tập trung vào phần còn lại: **vì sao tách `consumer` thành service riêng**, cách Docker Compose ghép service với code, giải phẫu `consumer/consumer.py`, và ClickHouse cơ bản. Toàn bộ nội dung đã test thật trong session (không phải lý thuyết suông) — có DLQ trigger thật bằng cách tắt ClickHouse, có recovery thật.

## 1. Vì sao BẮT BUỘC tách `consumer` thành service riêng, không nhét vào `gateway`

Đây là câu hỏi cốt lõi nhất, và bằng chứng đã tự tay kiểm chứng: lúc `docker stop clickhouse`, **gateway vẫn trả 200 bình thường** cho mọi webhook — không hề bị ảnh hưởng.

**Lý do gốc rễ — rủi ro thật từ chính doc Pancake đã đọc**: Pancake tự động **suspend webhook** nếu trong 30 phút có >80% request lỗi và ≥300 request lỗi (timeout/status ngoài 2xx/network error tính là lỗi). Nếu gateway phải chờ ghi ClickHouse xong mới trả response cho Pancake, thì **bất kỳ lúc nào ClickHouse chậm/chết, toàn bộ webhook cũng die theo** → dễ dính ngưỡng suspend đó.

```
KHÔNG tách (rủi ro):
Pancake --webhook--> [1 process vừa nhận HTTP vừa ghi ClickHouse]
                            ClickHouse chết -> webhook cũng chết theo -> Pancake suspend

CÓ tách (đang làm, đã test):
Pancake --webhook--> gateway (chỉ nói chuyện Kafka, luôn nhanh, gần như không bao giờ chết)
                          Kafka (kho chứa tạm, giữ message lại an toàn)
                                consumer (nói chuyện ClickHouse, CHẾT TẠM CŨNG ĐƯỢC)
                                     ClickHouse chết -> consumer tự retry/DLQ -> gateway không biết gì cả
```

**Kafka ở giữa = bộ đệm ngắt kết nối giữa 2 tốc độ khác nhau**: gateway cần nhanh + luôn sẵn sàng (Pancake đang chờ), việc ghi ClickHouse có thể chậm/lỗi/retry thoải mái mà không ảnh hưởng phía nhận webhook. Đây là lý do gốc rễ toàn bộ kiến trúc dùng Kafka, không phải "cho oai".

**Về mặt kỹ thuật thuần túy**: `gateway` là 1 process chạy `uvicorn` (chờ HTTP request tới thì xử lý), `consumer` là 1 process khác hẳn (vòng lặp `while True: poll()` chạy vô tận, tự chủ động đi lấy dữ liệu). 2 kiểu chạy khác nhau, chuẩn Docker là **1 container = 1 process chính** — không gộp chung được.

## 2. Docker Compose ghép "service" với "code" bằng gì

Tên service (`gateway`, `consumer`) **chỉ là 1 cái nhãn** — dùng làm DNS name trong network nội bộ Docker, **không hề liên quan tới việc chạy file Python nào**. Thứ thật sự quyết định là dòng `command:`:

```yaml
gateway:
  build: .              # build từ CÙNG 1 Dockerfile với consumer
  # không có command: -> dùng CMD mặc định trong Dockerfile: uvicorn gateway.main:app

consumer:
  build: .               # cùng image hệt gateway
  command: ["python", "-m", "consumer.main"]   # <- override CMD, chạy file khác
```

`gateway` và `consumer` **dùng chung 1 Docker image** (`Dockerfile` copy cả 2 thư mục `gateway/` và `consumer/` vào cùng 1 image) — chỉ khác đúng ở `command:` lúc container khởi động, chọn "chạy entrypoint nào". Nếu quên `command:` ở service `consumer`, nó sẽ chạy nhầm `uvicorn gateway.main:app` dù tên service là "consumer" — tên không tự động ánh xạ sang code.

## 3. Ranh giới module — vì sao `consumer` không được import từ `gateway`

```
gateway/                 consumer/
  __init__.py               __init__.py
  main.py   <- route HTTP    main.py    <- chỉ gọi consumer_event()
  producer.py <- Kafka       consumer.py <- Kafka + ClickHouse
```

Quy tắc: **cấu trúc thư mục phản chiếu đúng ranh giới deploy**. Mỗi thư mục cấp cao nhất = 1 process độc lập. Cái gì chỉ 1 service dùng thì nằm bên trong service đó (`producer.py` chỉ `gateway` cần, vì chỉ gateway produce). `consumer` cần tự có `Producer` riêng cho DLQ ([consumer/consumer.py:52](consumer/consumer.py#L52)) — không tái dùng `_producer` bên `gateway/producer.py`, vì đó là 2 process khác nhau, không chia sẻ được biến trong RAM giữa 2 container.

## 4. Giải phẫu `consumer/consumer.py` — đọc theo đúng thứ tự CHẠY, không phải thứ tự viết

Bắt đầu từ `consumer_event()` ([dòng 127](consumer/consumer.py#L127)) — hàm thật sự chạy khi container start.

**Bước A — setup 1 lần** (dòng 128-130): `_ensure_schema()` tạo database/table nếu chưa có; `_consumer.subscribe([RAW_TOPIC])` đăng ký đọc topic `pancake.raw`.

**Bước B — vòng lặp vô tận** (dòng 136-156):
```python
msg = _consumer.poll(1.0)   # (137)
```
Lưu ý quan trọng: `poll()` bên `Consumer` **khác nghĩa hoàn toàn** với `poll()` bên `Producer` (đã học ở `gateway/producer.py`). Bên producer, `poll()` chỉ "nghe callback" của việc gửi đã âm thầm chạy nền từ trước. Bên đây, `poll()` là **hành động chủ động đi hỏi Kafka "có gì mới không"** — không gọi thì không có message nào tới cả. Cùng tên hàm, 2 class khác nhau, 2 cơ chế khác nhau.

Message nhận được chỉ được **gom vào 1 list** (dòng 143-148), chưa xử lý gì — xử lý thật khi đủ điều kiện flush (dòng 153: đủ `BATCH_SIZE` **HOẶC** quá `BATCH_FLUSH_SECONDS`).

**Bước C — `_flush_batch`** (dòng 102-124): người điều phối, không tự làm việc nặng.
```python
for attempt in range(1, 4):        # thử tối đa 3 lần
    try:
        _process_batch(batch)      # việc nặng thật (ghi ClickHouse) nằm ở đây
        success = True; break
    except Exception as exc:
        ...; time.sleep(1)

if success:
    _consumer.commit(...)          # (119) báo Kafka "xử lý xong, đừng gửi lại"
else:
    _send_to_dlq(batch, ...)       # (123) hết 3 lần vẫn lỗi -> đẩy sang DLQ
    _consumer.commit(...)          # (124) VẪN commit -- không thì Kafka gửi lại batch lỗi này MÃI MÃI
```

**Bước D — `_process_batch`** (dòng 78-99): **việc chính, lý do consumer tồn tại** — insert batch vào ClickHouse. Cố ý **không có try/except** trong hàm này (đọc comment dòng 88-89): lỗi phải tự `raise` lên cho `_flush_batch` bắt được để retry đúng cơ chế — nếu bắt exception ngay tại đây, lỗi "biến mất" trước khi tới nơi cần xử lý.

**Điểm hay hiểu lầm nhất**: `_send_to_dlq` ([dòng 67-75](consumer/consumer.py#L67-L75)) **không phải "1 trong 2 việc chính"** ngang hàng với consume — nó chỉ là **lối thoát hiểm** khi bước D thất bại. Việc chính duy nhất là: đọc Kafka → ghi ClickHouse. Nếu bỏ hẳn ClickHouse đi, consumer không còn lý do tồn tại.

## 5. ClickHouse — vừa đủ để hiểu code đang chạy

- **Columnar OLAP database** — tối ưu cho quét/tổng hợp nhiều dòng (báo cáo, phân tích), không phải để sửa/xoá từng dòng như Postgres.
- **MergeTree** — engine chính, dữ liệu được sắp xếp vật lý theo `ORDER BY` lúc ghi. Bảng `raw.pancake_raw` dùng `ORDER BY (_ingested_at, _kafka_offset)` — **chưa dùng `ReplacingMergeTree`** (engine hay dùng để dedup) vì chưa có business key thật (`conversation_id`) để dedup có ý nghĩa; việc đó để dành lúc tách bảng theo event type thật.
- **2 cổng khác giao thức, dễ nhầm**: `8123` = HTTP interface (thư viện `clickhouse-connect` đang dùng), `9000` = native protocol (thư viện khác như `clickhouse-driver` mới dùng). Lúc mới thêm ClickHouse, code từng để nhầm `CLICKHOUSE_PORT: 9000` (copy từ biến thừa bên `gateway`) — sai giao thức, sẽ không kết nối được với `clickhouse-connect`. Đã sửa thành `CLICKHOUSE_HTTP_PORT: 8123`.
- **`client.insert(table, data=rows, column_names=[...])`** — `rows` phải là **list of list**, không phải list of dict.

## 6. Đã test thật những gì (bằng chứng, không phải giả định)

| Test | Cách làm | Kết quả |
|---|---|---|
| Insert thành công | curl POST vào gateway, đợi batch flush | Query ClickHouse thấy đúng payload, đúng offset |
| DLQ trigger thật | `docker stop clickhouse`, bắn thêm request | Log thấy `attempt=1,2,3` retry, rồi `sending to DLQ`; message thật nằm trong topic `pancake.raw.dlq`; **consumer không crash** |
| Tự phục hồi | `docker start clickhouse` lại, bắn request mới | Batch tiếp theo tự insert bình thường, không cần restart consumer |
| Resume đúng offset | `docker restart consumer` | Không đọc lại các offset đã commit trước đó |

## 7. Lệnh debug nhanh

```bash
# Log consumer real-time
docker logs -f consumer

# Xem bảng/data trong ClickHouse
docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM raw"
docker exec clickhouse clickhouse-client -q "SELECT * FROM raw.pancake_raw ORDER BY _kafka_offset"

# Xem message rơi vào DLQ
docker exec kafka-kraft /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pancake.raw.dlq --from-beginning
```

## 8. Vài hiểu lầm đã tự mắc phải — ghi lại để khỏi lặp lại

1. **"`command:` xảy ra trước/sau lúc build"** — sai khái niệm thời điểm. **Build** chỉ tạo ra **1 image duy nhất** dùng chung cho cả `gateway` lẫn `consumer`, không "chạy" gì cả. Quyết định "dùng `command:` hay `CMD` mặc định trong Dockerfile" xảy ra ở bước **container start** (run), và xảy ra **cùng lúc** cho cả 2 service — không phải cái trước cái sau.
2. **"`uvicorn gateway.main:app` phải khớp tên service `gateway` trong compose"** — sai. `gateway.main:app` là cú pháp import Python (`tên_thư_mục.tên_file:tên_biến`) trỏ tới file thật trên đĩa container (`/app/gateway/main.py`), hoàn toàn tách biệt khỏi tên service khai trong `docker-compose.yml`. Trùng chữ "gateway" ở cả 2 chỗ chỉ vì tự đặt tên giống nhau cho dễ nhớ, không phải yêu cầu kỹ thuật — đổi tên service thành gì khác, dòng `uvicorn gateway.main:app` vẫn viết y nguyên.
3. **Bằng chứng thật của 2 điều trên**: từng comment `command:` của `consumer` để test, container tên "consumer" liền chạy nhầm `CMD` mặc định (`uvicorn gateway.main:app`) và crash với `KeyError: 'PANCAKE_WEBHOOK_SECRET'` — vì code gateway cần biến đó, nhưng `environment:` của service `consumer` không hề khai biến này. Tên container không quyết định code chạy; `environment:` cũng là cấu hình **riêng theo từng service**, không tự "theo" code đang chạy bên trong.
4. **"`consumer.poll()` đọc liên tục nhiều message 1 lúc"** — không chính xác. 1 lần gọi `poll()` chỉ lấy về **tối đa 1 message** (hoặc `None` nếu chưa có gì). Cái "liên tục" tới từ vòng lặp `while True` ở `consumer_event()` gọi `poll()` lặp đi lặp lại, không phải bản thân `poll()` tự động đọc nhiều message cùng lúc.
5. **"`range(1, 4)` nghĩa là retry 4 lần"** — sai, `range(1, 4)` cho ra `[1, 2, 3]` — đúng **3 lần**, cận trên (`4`) luôn bị loại trừ trong Python `range()`. Lỗi đếm off-by-one kinh điển.
6. **"Kafka nhận xong message thì ClickHouse có data ngay lập tức"** — sai, luôn có độ trễ tối đa `BATCH_FLUSH_SECONDS` giây ở giữa (consumer cố tình gom batch trước khi insert, không insert từng dòng ngay). Bắn request test xong rồi check ngay lập tức (chưa đủ vài giây) dễ tưởng nhầm "consumer không chạy gì" — thật ra nó chỉ đang gom batch, chưa tới lúc flush.

## 9. Lệnh debug nhanh (tiếp — kiểm tra service nào đang chạy code gì)

```bash
# Xem service nào build từ Dockerfile nào, command thật sự là gì (sau khi compose merge)
docker compose config

# Xem container có đang crash-loop không (STATUS phải là "Up", không phải "Restarting")
docker ps -a --filter name=consumer --format '{{.Names}}: {{.Status}}'
```

Liên quan: [webhook-fastapi-kafka-producer.md](webhook-fastapi-kafka-producer.md) (webhook/FastAPI/producer/Tunnel), [kafka-listeners.md](kafka-listeners.md) (networking tầng Kafka broker), [plan/pancake-streaming-week1.md](../plan/pancake-streaming-week1.md) (kiến trúc tổng + milestone).
