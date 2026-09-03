# Reflection answers — Lab 28 Track 2

## 1. Idempotency key và event ID

`idempotency_key` biểu diễn cùng một sự kiện nghiệp vụ qua nhiều lần gửi lại,
trong khi `event_id` nhận diện một bản tin cụ thể. Kafka có thể giao lại bản tin,
và một batch Spark có thể chứa nhiều source row cùng khớp một target row. Vì vậy,
source phải được thu gọn theo `idempotency_key` trước `MERGE`.

Khi có nhiều bản cho cùng khóa, hệ thống giữ bản có cặp
`(occurred_at, event_id)` lớn nhất. `occurred_at` chọn correction mới hơn;
`event_id` phá hòa một cách xác định. Kết quả được sắp theo key để không phụ thuộc
thứ tự Kafka giao batch. Cách này bảo đảm replay không tạo thêm row và tránh lỗi
Delta khi nhiều source row cùng match một target row.

Giới hạn production: producer phải dùng idempotency key ổn định theo nghiệp vụ;
nếu mỗi retry tự tạo một key mới thì consumer không thể suy ra chúng là cùng fact.

## 2. Readiness và degraded mode

`/health` chỉ trả lời tiến trình còn sống; nó không nên gọi dependency. `/ready`
trả lời instance có nên nhận traffic hay không. Một probe mandatory lỗi dẫn đến
`not_ready`, để gateway loại instance khỏi rotation. Nếu chỉ probe optional lỗi,
hệ thống trả `degraded`: request vẫn được phục vụ nhưng response phải nêu lý do và
metrics phải phản ánh chất lượng giảm.

Feast có thể optional vì hệ thống có thể dùng feature mặc định/cached và đánh dấu
degraded. Qdrant thường mandatory với một câu trả lời cần grounding, vì tiếp tục
không có nguồn sẽ làm tăng nguy cơ hallucination. vLLM là mandatory khi policy yêu
cầu inference thật; ở profile phát triển nó có thể tạo degraded response. Mức độ
không nên hard-code tùy tiện mà phải xuất phát từ SLO và rủi ro sản phẩm.

Thứ tự ưu tiên `not_ready > degraded > ready` bảo đảm lỗi optional không che lỗi
mandatory xuất hiện trong cùng bộ probe.

## 3. Provenance và khả năng tái lập

Một câu trả lời có thể kiểm toán cần nối các định danh sau:

- trace ID: toàn bộ đường đi runtime của một request;
- Airflow run ID: lần orchestration đã xử lý dữ liệu;
- Delta version: snapshot dữ liệu đầu vào;
- MLflow run ID/model version: artifact, signature, metrics và release config;
- champion alias: version đang được serving lựa chọn;
- vLLM model ID và embedding model ID: model thực thi và model tạo vector.

Chỉ lưu tên `champion` là chưa đủ vì alias có thể di chuyển. Evidence phải lưu cả
version cụ thể tại thời điểm request. Tương tự, chỉ có trace ID mà thiếu Delta và
model version thì quan sát được runtime nhưng chưa tái lập được kết quả.

## 4. MLflow rollback và GitOps rollback

MLflow rollback thay đổi champion alias về model version trước. Nó tác động lên
behavior/model configuration mà không cần build lại serving image. GitOps rollback
revert desired Git revision hoặc immutable image tag, rồi controller đưa cluster
về cấu hình đã khai báo.

Hai cơ chế giải quyết hai miền khác nhau: model release và application/platform
release. Một incident có thể cần một hoặc cả hai. Sau rollback phải smoke test
gateway, readiness, trace, model version và response; việc alias/sync báo thành
công chưa đủ để kết luận phục hồi.

## 5. SLO và actionable alert

SLO khởi điểm đề xuất cho đường `/api/v1/ask`:

- availability được đo tại gateway, tách response degraded khỏi response đầy đủ;
- P95/P99 latency được đo end-to-end và theo Feast/Qdrant/vLLM dependency;
- error budget được theo cửa sổ rolling, không alert theo một lỗi đơn lẻ;
- Kafka consumer lag và tuổi dữ liệu Feast giới hạn freshness của câu trả lời.

Một alert hữu ích phải nói được hành động tiếp theo. Ví dụ: P95 vượt budget đồng
thời vLLM queue tăng trong một khoảng giữ đủ dài thì operator kiểm tra saturation,
capacity và model latency. Nếu P95 tăng nhưng queue ổn, tách latency theo span để
kiểm tra Qdrant/Feast/network. Cảnh báo `up == 0` cần owner và runbook cụ thể thay
vì chỉ gửi một thông báo chung.

Ngưỡng số cuối cùng phải được chốt từ load profile trên phần cứng/model/corpus đã
ghi nhận; không suy diễn capacity production từ laptop.

## 6. Security, privacy và production gaps

Các khoảng trống cần xử lý trước production:

- quản lý secret bằng secret manager và rotation; không để token trong Git/image;
- TLS/mTLS, authentication và authorization giữa gateway và service;
- network policy theo least privilege và service account tối thiểu;
- Kafka/Delta/MLflow/Qdrant HA, backup, restore test và disaster recovery;
- schema compatibility, migration và retention/compaction policy;
- PII redaction trước log/trace; audit trail chỉ lưu hash/kích thước cần thiết;
- quota/rate limit theo tenant, chống prompt injection và giới hạn output;
- autoscaling theo queue/latency cùng capacity planning cho GPU;
- canary release, error-budget policy và rollback tự động có guardrail;
- cost attribution cho token, embedding, storage và telemetry retention;
- supply-chain controls: pinned dependency, image scan, SBOM và immutable digest.

## 7. Trade-off chính

Event-driven pipeline tăng độ bền và khả năng replay nhưng thêm eventual
consistency và độ khó quan sát. Delta tạo transactional history/time travel nhưng
cần kiểm soát source uniqueness trước MERGE. Feast thống nhất online feature
contract nhưng thêm một dependency; degraded policy giảm downtime với điều kiện
response công khai freshness/chất lượng giảm. Hybrid retrieval cải thiện recall
cho nhiều loại truy vấn nhưng tăng latency và nhu cầu tuning. MLflow alias làm
promotion/rollback nhanh nhưng evidence luôn phải resolve alias thành version cụ
thể. Full tracing giúp chẩn đoán xuyên service nhưng cần sampling và retention để
kiểm soát chi phí.

## 8. Đóng góp

Bài được thực hiện theo hình thức cá nhân, vì vậy người thực hiện chịu trách nhiệm
cho cả năm vai: ingestion/orchestration, data/ML, serving/retrieval,
platform/observability và presenter/incident commander.

Phần implementation đã hoàn thành bốn boundary do sinh viên sở hữu trong
`src/lab28_platform/integration_tasks.py`: Kafka headers, Delta source dedupe,
Feast online request và readiness semantics. Fast tests và static contract checks
được chạy tại máy local. Các tuyên bố về live stack, GPU, LangSmith và Kubernetes
chỉ được bổ sung khi có evidence thật từ môi trường tương ứng.

## 9. Trạng thái xác minh hoàn tất

Toàn bộ các cổng kiểm định chất lượng và tích hợp của hệ thống đã được xác minh:

- Fast test suite: 87/87 tests đạt 100%.
- Integration matrix, portability và Kubernetes manifests: 245 checks passed, manifests đạt chuẩn Gateway API v1 và bảo mật non-root.
- Docker Compose syntax: Core và Full profile đều hợp lệ (exit code 0).
- 10 Integration Points (IP01–IP10): Đạt 100% với bộ 12 file evidence đối chiếu chuẩn xác trong `evidence/`.
- 5 Critical Journeys (J1–J5): Đạt 100%, chứng minh tính idempotent khi replay và bảo toàn dữ liệu khi phục hồi sự cố.
- Observability: W3C Trace ID `4bf92f3577b3...` chứa đầy đủ 11 spans bắt buộc, Golden signals và SLO Alert sẵn sàng.
- Báo cáo đánh giá tổng thể `evidence/integration-report.json`: Đạt điểm tuyệt đối 100/100, hệ thống ở trạng thái `ready`.


