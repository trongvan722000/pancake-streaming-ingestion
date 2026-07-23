# Kafka Listener — Kiến thức tổng hợp

Note này đúc kết từ quá trình tự debug thật trên project (Kafka KRaft trong `docker-compose.yml`), không phải lý thuyết suông — mọi kết luận đều đã test bằng `kafkacat` từ cả host lẫn container.

## 1. Vì sao Kafka cần khái niệm "listener"

Khác với DB thường (connect 1 phát là xong), Kafka bắt buộc qua **2 bước**:

1. Client **bootstrap** vào 1 địa chỉ bất kỳ để xin **metadata** (topic nào, partition nào, leader là broker nào).
2. Broker trả lời, trong đó nhét kèm **địa chỉ thật để connect tiếp** — địa chỉ này lấy từ `advertised.listeners` mà broker tự khai, **không phải** địa chỉ client vừa gõ.

Vấn đề: **host** (máy Mac) và **container khác** (gateway, consumer, kafka-ui...) cần 2 địa chỉ hoàn toàn khác nhau để với tới cùng 1 Kafka broker. Vì bước 2 chỉ trả được **1 câu trả lời cố định** cho mỗi listener, Kafka phải hỗ trợ khai **nhiều listener song song**, mỗi listener phục vụ đúng 1 nhóm "khán giả".

## 2. Ba dòng config, ba vai trò khác nhau — không được lẫn

| Config | Vai trò | Ràng buộc |
|---|---|---|
| `KAFKA_LISTENERS` | Bind vật lý — socket nào thật sự mở trên OS | Port phải **khác nhau** giữa các listener (1 process không bind 2 lần cùng port) |
| `KAFKA_ADVERTISED_LISTENERS` | Địa chỉ Kafka "quảng cáo"/trả về ở bước 2 cho từng listener | Chỉ là string — **có thể trùng nhau** giữa các listener, sai thì không crash, chỉ redirect sai |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Loại bảo mật ứng với **tên** listener (PLAINTEXT/SSL/SASL...) | Tên listener (`PLAINTEXT`, `INTERNAL`, `CONTROLLER`) là do mình tự đặt tuỳ ý — trùng chữ với protocol type `PLAINTEXT` (nghĩa là "không mã hoá") chỉ là trùng tên, 2 khái niệm khác nhau |

Config thực tế của project (đã fix đúng):
```yaml
KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094'
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092,INTERNAL://kafka:9094'
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT'
```
Trong `ports:` của compose chỉ publish `"9092:9092"` ra host — `9093`/`9094` không publish, vì không audience nào ở host cần dùng chúng.

## 3. Quy tắc chọn địa chỉ cho client — Kafka không hề "nhận diện" ai gọi

Không có logic if/else nào trong Kafka phân biệt host vs container. Cơ chế **tĩnh**: gõ cửa (port) nào thì rơi vào đúng listener đó, listener luôn trả lời y chang câu đã cấu hình sẵn, bất kể ai hỏi. "Thông minh" nằm ở người viết client config, không nằm ở Kafka lúc runtime.

Quy tắc: hỏi **"process này chạy ở đâu?"**
- Ngoài host (Mac, ngoài Docker) → `localhost:<port đã publish trong "ports:">`
- Trong container cùng compose network → `<service_name>:<port INTERNAL>` (ví dụ `kafka:9094`)

Áp dụng vào project — bảng địa chỉ cho từng service:

| Service | Chạy ở đâu | Bootstrap phải dùng |
|---|---|---|
| `kcat` gõ tay từ Mac | Host | `localhost:9092` |
| `kafka-ui` | Container | `kafka:9094` |
| `gateway` (FastAPI, M2) | Container | `kafka:9094` |
| consumer app (M3) | Container | `kafka:9094` |

## 4. Docker networking — vì sao container "thấy" nhau mà không cần `ports:`

Mỗi container có network namespace riêng (localhost của container A ≠ localhost của container B ≠ localhost của host). Docker Compose tạo 1 mạng ảo (bridge network) cho phép các container gọi nhau qua **tên service** (DNS nội bộ của Docker).

Điểm hay bị hiểu lầm nhất: dòng `ports: "9092:9092"` **chỉ ảnh hưởng tới client từ host** — nó publish/NAT port đó ra ngoài máy thật. Container khác trong cùng network **không đi qua cơ chế này** — nó gọi thẳng vào container kia qua IP nội bộ + đúng port mà container đó bind (`0.0.0.0:<port>`), hoàn toàn không cần port đó có nằm trong `ports:` hay không.

## 5. Năm loại lỗi gốc — khác tầng, khác triệu chứng, khác cách phát hiện

| # | Tình huống | Tầng hỏng | Cơ chế | Triệu chứng |
|---|---|---|---|---|
| 1 | Host gọi theo tên service (`kafka:...`) | DNS, trước cả TCP | Host không tham gia Docker DNS nội bộ, không biết `kafka` là ai | Lỗi ngay, rõ ràng ("failed to resolve") |
| 2 | Host gọi `localhost:<port chưa publish>` | Docker networking (OS) | Port đó chưa từng được map trong `ports:` | Refuse tức thì, không liên quan gì tới Kafka |
| 3 | Container bị advertised-listener trỏ về `localhost` | Logic Kafka, ở bước redirect (bước 2) | Bootstrap **thành công** (vì port đang gõ vẫn mở trong docker network), nhưng lúc produce/consume thật, client tự nối lại theo địa chỉ advertise → `localhost` trong chính container đó = chính nó → refuse, nhưng client (librdkafka) **retry ngầm liên tục** | Không lỗi ngay, **giống bị treo**, nguy hiểm nhất vì khó phát hiện |
| 4 | Client thường (kafkacat) nói chuyện với listener `CONTROLLER` | Tầng giao thức | TCP bắt tay được (đúng địa chỉ, đúng port đang mở), nhưng `CONTROLLER` nói 1 sub-protocol khác hẳn (Raft, dành cho broker/controller nói chuyện nội bộ lúc bầu cử/đồng bộ KRaft), không phải giao thức Produce/Fetch/Metadata mà client thường biết | Connect được nhưng nhận phản hồi không giải mã được / lỗi protocol |
| 5 | Không set `KAFKA_ADVERTISED_LISTENERS`, để Kafka tự fallback = `KAFKA_LISTENERS` (chứa `0.0.0.0`) | Startup validation | Kafka có bước validate cứng lúc boot: **cấm advertised address là `0.0.0.0`** (vì đó là địa chỉ bind wildcard, vô nghĩa với client connect tới) | Kafka **từ chối khởi động luôn**, chưa kịp mở port nào — khác hẳn 4 loại kia (đều xảy ra khi Kafka đã chạy) |

**Điểm mấu chốt xuyên suốt cả 5 loại**: cái quyết định sống-chết không phải "port có đúng số hay không", mà là **hostname trong địa chỉ advertise có tồn tại/với-tới-được với đúng người hỏi hay không**. Container nào cũng thấy được mọi port mà Kafka bind `0.0.0.0` trong cùng network (không có tường lửa giữa các container) — số port chỉ để tách các listener ra khi bind trong 1 process, không phải rào chắn mạng.

## 6. Cách test đúng — đừng tin `-L`

`-L` (list metadata) là phép test **yếu**, có thể đánh lừa — nó chỉ đọc dữ liệu có sẵn trên kết nối bootstrap, không nhất thiết phải reconnect qua advertised address thật để in ra danh sách topic. Bằng chứng thật đã test: gọi vào listener sai (`kafka:9092` từ trong container) vẫn in được đầy đủ danh sách topic, chỉ khác là hiện `broker -1` (chưa xác nhận reconnect) thay vì `broker 1` (đã xác nhận).

**Muốn biết listener có thật sự đúng, bắt buộc test bằng `-P` (produce) hoặc `-C` (consume)** — 2 lệnh này ép mở kết nối mới tới đúng leader broker theo advertised address, lỗi (hoặc treo) sẽ lộ ra ngay.

## 7. Lệnh tham khảo nhanh

```bash
# Tạo topic — exec thẳng vào container Kafka, dùng localhost vì đang đứng trong chính nó
docker exec -it kafka-kraft /opt/kafka/bin/kafka-topics.sh \
  --create --topic <ten-topic> --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Test từ HOST (cần cài: brew install kcat)
kcat -b localhost:9092 -L                      # xem metadata
kcat -b localhost:9092 -t <topic> -P            # produce (Ctrl-D để gửi)
kcat -b localhost:9092 -t <topic> -C            # consume

# Test từ CONTAINER khác (dùng service debug, image confluentinc/cp-kafkacat)
docker compose exec debug kafkacat -b kafka:9094 -L
docker compose exec debug kafkacat -b kafka:9094 -t <topic> -P
docker compose exec debug kafkacat -b kafka:9094 -t <topic> -C

# Test TCP thô (không cần cài gì, dùng bash built-in) — chỉ chứng minh network thông, KHÔNG chứng minh protocol đúng
docker exec -it <container> bash -c 'echo > /dev/tcp/kafka/9094 && echo OK || echo FAILED'
```

## 8. Checklist khi thêm 1 service (container) mới cần nói chuyện với Kafka

1. Service đó có nằm trong cùng `docker-compose.yml`/network với `kafka` không? → nếu có, luôn dùng `kafka:9094` (INTERNAL), không bao giờ dùng `9092`.
2. Đưa `bootstrap.servers` vào **biến môi trường** (`KAFKA_BOOTSTRAP_SERVERS`), không hardcode trong code — để dễ đổi khi chạy ở môi trường khác (vd chạy tay ngoài Docker để debug nhanh thì đổi thành `localhost:9092`).
3. Sau khi thêm xong, **không tin `-L`** — test thật bằng produce/consume (hoặc chính luồng thật của app) trước khi coi là xong.
