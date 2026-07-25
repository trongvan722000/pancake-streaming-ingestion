# Webhook → FastAPI → Kafka Producer — Kiến thức tổng hợp

Note này đúc kết từ quá trình build + test thật gateway nhận webhook Pancake (`gateway/main.py`), kèm các bài tập tự tay làm ở `learn-fastapi/` để nắm HTTP/FastAPI/Kafka producer từ gốc. Có cả phần "hiểu sai lúc đầu — sửa lại" để lần sau đọc khỏi lặp lại nhầm lẫn cũ.

## 1. HTTP request/response — nền tảng của mọi thứ

Client gửi **request** (method + path + có thể kèm body), server trả **response** (status code + body).

| Method | Ý nghĩa | Dùng ở đâu trong project |
|---|---|---|
| GET | "lấy gì đó" / kiểm tra sống | `verify_webhook` — Pancake/ai đó test xem URL có sống không |
| POST | "gửi gì đó cho server xử lý" | `receive_webhook` — Pancake gửi event thật (JSON body) |

Status code hay gặp: `200` = ok, `404` = route không tồn tại (hoặc cố tình giả vờ không tồn tại), `405` = method không khớp với route đã đăng ký, `422` = body gửi lên sai schema (khi dùng Pydantic validate).

## 2. Webhook là gì

**Webhook = thay vì server của mình liên tục hỏi "có gì mới chưa?" (polling), bên kia (Pancake) tự POST sang server của mình ngay khi có event.**

Vai trò 2 phía, không nhầm lẫn:
- **Pancake** = sender — chỉ biết 1 thứ duy nhất: URL webhook đã đăng ký. Không biết, không cần biết phía sau URL đó xử lý ra sao (Kafka, DB, gì cũng được).
- **Gateway (FastAPI)** = receiver — vừa là nơi nhận webhook, vừa (trong code của mình) đóng luôn vai trò gọi Kafka producer.

Tự mô phỏng lại bằng 2 file Python độc lập (`app.py` đóng vai gateway, `sender.py` đóng vai Pancake gọi `requests.post(...)` định kỳ) là cách nhanh nhất để thấy rõ 2 vai trò này tách biệt nhau thế nào — sender không hề biết receiver làm gì với data sau khi nhận.

## 3. FastAPI — cơ chế route

```python
@app.get("/hello/{name}")
def say_hello(name: str):
    ...
```

- `@app.get(...)` / `@app.post(...)`: khai báo **1 cặp (method, path)** gắn với 1 hàm xử lý. FastAPI khớp request đến theo đúng cả 2 điều kiện — sai method dù đúng path vẫn báo lỗi (405), không tự động dùng route khác.
- `{name}` trong path = **path parameter**, FastAPI tự tách phần đó trong URL thật ra thành biến truyền vào hàm. Đây chính là cơ chế `{secret_token}` trong `/webhooks/pancake/{secret_token}` đang dùng.
- `async def` + `await request.body()`: đọc raw bytes của request. Muốn ép schema chuẩn (bắt buộc field nào, kiểu gì) thì khai Pydantic model làm tham số thay vì tự `json.loads` tay — sai schema tự động trả `422` không cần code thêm.

## 4. Kafka producer — 3 bước tách biệt, đừng gộp lại

Đây là chỗ dễ hiểu lầm nhất — nhầm lẫn ban đầu: tưởng `poll()` là bước "lưu vào broker". **Sai.**

| Bước | Ai làm | Việc thật sự |
|---|---|---|
| `producer.produce(topic, key, value, callback=...)` | Code gọi | Bỏ message vào **buffer RAM nội bộ**, return ngay lập tức. Đã "âm thầm" bắt đầu quá trình gửi. |
| Background thread (bên trong thư viện `confluent-kafka`, tự chạy) | Thư viện tự làm ngầm | **Đây mới là bước thật sự gửi qua network tới Kafka broker.** Không do `poll()` kích hoạt. |
| `producer.poll(0)` | Code gọi | Chỉ hỏi "tin báo (callback) nào đã có sẵn, xử lý ngay cho tao" — nghe kết quả, KHÔNG phải hành động gửi. |
| `producer.flush(timeout)` | Code gọi (thường lúc shutdown) | **Chặn lại**, ép mọi message còn trong buffer phải gửi xong (hoặc timeout) trước khi cho phép thoát. Khác `poll()` ở chỗ nó blocking và đảm bảo hết buffer. |

Hệ quả nếu quên gọi `poll()` định kỳ: buffer đầy dần → tới lúc `produce()` sẽ raise `BufferError` (tự tay làm tràn buffer ở bài tập để thấy tận mắt).

Hệ quả nếu quên `flush()` lúc shutdown: message còn kẹt trong buffer **mất luôn** khi process chết, vì background thread chưa kịp gửi.

**`key` khi produce dùng để làm gì**: Kafka dùng `key` để chọn partition — cùng key luôn rơi cùng 1 partition, và Kafka đảm bảo thứ tự **trong 1 partition**. Dùng `conversation_id` làm key → toàn bộ tin nhắn của 1 conversation luôn xử lý đúng thứ tự dù topic chạy nhiều partition song song. Không set key → rải ngẫu nhiên, mất thứ tự giữa các message cùng conversation.

## 5. Luồng thật trong `gateway/main.py`

```
Pancake --POST--> /webhooks/pancake/{secret_token}
    │
    ├─ so khớp secret_token, sai thì 404 (giả vờ route không tồn tại)
    │
    ├─ await request.body() → json.loads() (lỗi JSON thì vẫn lưu raw text, không mất data)
    │
    ├─ _extract_key(): tìm conversation_id/conversationId/id để làm Kafka key
    ├─ _classify_topic(): hiện return cứng "pancake.raw" (chưa biết field thật của Pancake để phân loại type)
    │
    ├─ producer.produce(topic, key, value, callback=_delivery_report)
    ├─ producer.poll(0)
    │
    └─ return {"status": "ok", "request_id": ...}   # trả 200 NGAY, KHÔNG đợi Kafka ACK xong
```

Đánh đổi có chủ đích: trả response nhanh cho Pancake (nhiều webhook có timeout ngắn), chấp nhận rủi ro nếu Kafka down đúng lúc đó thì message chỉ log lỗi, chưa có retry/DLQ ở tầng gateway (việc cần làm thêm nếu muốn production-grade).

## 6. Cloudflare Tunnel — vì sao cần, cơ chế thật sự

**Vấn đề**: máy mình không có IP public, Pancake ở ngoài internet không gõ được vào `localhost:8000`.

**Cách Tunnel giải quyết — KHÔNG phải NAT port-forwarding** (hiểu lầm hay gặp):
- NAT port-forwarding truyền thống = mở 1 cửa trên router, ai cũng gõ vào được — cần cấu hình router, rủi ro bảo mật.
- Cloudflare Tunnel = `cloudflared` trên máy mình tự mở **kết nối đi ra ngoài (outbound)** tới Cloudflare — không port nào bị mở từ phía router/máy mình cả. Khi có request tới URL public, Cloudflare đẩy ngược nó qua đúng đường ống outbound đã mở sẵn đó.

```
Internet --> Cloudflare edge --> (đường ống outbound do cloudflared tự mở trước) --> cloudflared trên máy mình --> localhost:8000 --> gateway
```

| | Quick tunnel (`cloudflared tunnel --url ...`) | Named tunnel |
|---|---|---|
| Cần Cloudflare account/domain | Không | Có |
| URL | Random (`xxx.trycloudflare.com`), đổi mỗi lần chạy | Cố định, domain riêng |
| Dùng để | Test nhanh, demo cho đồng nghiệp | Đăng ký thật với Pancake (cần URL không đổi) |

Lệnh hay dùng:
```bash
cloudflared tunnel --url http://localhost:8000   # chạy foreground, URL in ngay ra terminal, Ctrl+C để tắt
```

## 7. Vài hiểu lầm đã tự mắc phải — ghi lại để khỏi lặp lại

1. **"Domain tunnel xấu vì được tạo từ Kafka producer"** — sai, domain là do Cloudflare tự random, không liên quan Kafka. Kafka producer là code chạy **sau khi** request đã qua tunnel + qua FastAPI rồi.
2. **"Endpoint phải được tạo từ producer/connector của Kafka"** — có tồn tại loại tool làm được vậy thật (Kafka Connect Webhook Source Connector, xem `Platformatory/webhook-source-connector`), nhưng project này **cố tình không dùng** — tự viết FastAPI để luyện kỹ năng, không phải không biết cách khác.
3. **"poll() là bước lưu data vào broker"** — sai, xem lại mục 4. `produce()` mới là bước bắt đầu gửi (qua background thread), `poll()` chỉ là nghe kết quả.
4. **"Kafka và Pancake nói chuyện qua webhook"** — sai chủ thể. Webhook là giữa **Pancake và gateway**. Kafka không hề biết Pancake tồn tại, chỉ gateway biết cả 2 bên.
5. **"`_delivery_report` chỉ chạy/log khi có lỗi"** — sai, callback này xử lý **cả 2 nhánh**: thành công (`else: log "kafka delivered..."`) lẫn thất bại (`if err: log error`). Không phải "im lặng lúc thành công, chỉ lên tiếng lúc lỗi".
6. **"Producer cũng `poll()` liên tục như Consumer"** — nhầm 2 class khác nhau. Bên `gateway/producer.py`, `_producer.poll(0)` chỉ gọi **đúng 1 lần** trong mỗi request (`produce_event()`), không có vòng lặp `while True` nào ở phía producer cả. Vòng lặp poll liên tục là đặc điểm riêng của **Consumer** (xem [consumer-clickhouse-architecture.md](consumer-clickhouse-architecture.md) mục 4).
7. **"Producer gom message theo batch, cứ đủ N message mới gửi"** — sai, đó là khái niệm của **consumer** (`BATCH_SIZE=100`). Producer bên gateway gửi **ngay từng message một** mỗi khi có 1 request POST tới, không gom gì cả.
8. **"`bootstrap.servers` là nơi Kafka lưu message"** — sai, đó chỉ là **địa chỉ để client kết nối vào cluster** (xin metadata ban đầu). `topic` mới là nơi message thật sự được lưu; `broker` là 1 server cụ thể trong cluster giữ dữ liệu đó.
9. **"POST vào 1 path chỉ có route GET sẽ trả 404"** — sai, FastAPI trả **405 Method Not Allowed** (path tồn tại, chỉ sai method) — khác hẳn 404 (path không tồn tại, hoặc cố tình giả 404 khi secret token sai như `verify_webhook` đang làm).
10. **`HTTPException(status_code=404)` không truyền `detail=`** — FastAPI tự điền text mặc định theo status code, ra đúng body `{"detail": "Not Found"}` — là hành vi mặc định của framework, không phải bug.
11. **"Cùng partition nghĩa là cùng consumer group"** — 2 khái niệm hoàn toàn khác nhau, không liên quan gì tới nhau. **Partition** do `key` hash ra, quyết định message rơi vào "ngăn" nào trong 1 topic. **Consumer group** là cơ chế khác hẳn — nhiều consumer cùng group chia nhau đọc 1 topic. Cùng key → cùng partition; không suy ra được gì về consumer group từ đó cả.

## 8. Lệnh debug nhanh — test thật, đừng đoán

```bash
# Xem gateway có nhận request + produce thành công không (log real-time)
docker logs -f gateway

# Xem nội dung message thật đã nằm trong Kafka
docker exec -it kafka-kraft /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pancake.raw --from-beginning

# Test endpoint bằng tay (PHẢI dùng POST — mở link trên trình duyệt chỉ gọi GET, sẽ ra trang trắng vì route GET cố tình trả response rỗng)
curl -X POST "http://localhost:8000/webhooks/pancake/<secret>" \
  -H "Content-Type: application/json" -d '{"type":"message","conversation_id":"c1"}'

# Xem tunnel đang chạy chưa + URL hiện tại
ps aux | grep "[c]loudflared"
```

Liên quan: [notes/kafka-listeners.md](kafka-listeners.md) (networking/listener của chính Kafka broker — tầng dưới, khác với tầng producer ở note này), [plan/pancake-streaming-week1.md](../plan/pancake-streaming-week1.md) (kiến trúc tổng thể, các milestone còn lại).
