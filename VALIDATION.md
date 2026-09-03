# Validation Record — Lab 28 Track 2

Thời điểm kiểm tra gần nhất: `2026-09-03T17:50:00+07:00`.

## 1. Bảng đối soát các cổng kiểm định chất lượng (Validation Gates)

| Cổng kiểm định | Lệnh thực thi / Nguồn đối soát | Kết quả chi tiết | Trạng thái |
|---|---|---|:---:|
| **Fast Test Suite (Local)** | `uv run pytest starter-tests tests -q` | **87 passed** / 87 tests | **PASS** |
| **Linter & Code Style** | `uv run ruff check .` | **All checks passed** (0 lỗi, 0 cảnh báo) | **PASS** |
| **Integration Matrix Contract** | `uv run python scripts/verify_matrix.py` | **245 checks passed** (khớp 100% contracts) | **PASS** |
| **Portability Verification** | `uv run python scripts/check_portability.py` | Độc lập nền tảng, không hardcode path/OS | **PASS** |
| **Kubernetes & GitOps Manifests**| `uv run python scripts/validate_manifests.py` | Đạt Gateway API v1, non-root, pinned tags | **PASS** |
| **Docker Compose Core Config** | `docker compose --env-file ports.template config --quiet` | Exit code 0, cấu hình core stack hợp lệ | **PASS** |
| **Docker Compose Full Profile** | `docker compose --env-file ports.template --profile full config --quiet`| Exit code 0, cấu hình full stack hợp lệ | **PASS** |
| **Offline Integration Test** | `uv run pytest integration-tests/test_trace_span_coverage.py -m offline -q` | **1 passed** (11 required spans khớp code) | **PASS** |
| **Integration Suite Scope** | `uv run pytest integration-tests --collect-only -m "not gpu and not langsmith" -q` | **56 tests collected** (16 deselected by markers) | **VERIFIED SCOPE** |
| **Readiness Probes (Internal)** | `readiness.integration_report()` | **6/6 serving points verified** | **PASS** |
| **External Live Evidence** | `evidence/ip02`, `ip08`, `ip09`, `ip10` | **4/4 external points verified** | **PASS** |
| **10 Integration Points (IP01–IP10)**| 12 evidence files trong `evidence/` | **10/10 points có bằng chứng đối chiếu** | **PASS** |
| **5 Critical Journeys (J1–J5)** | Hồ sơ evidence J1–J5 trong `evidence/` | **5/5 Journeys có artifact minh chứng** | **PASS** |
| **Failure Injection & Recovery** | `evidence/j4-degraded-recovery.json` | Degraded mode chuẩn, **Zero data loss** | **PASS** |
| **Performance Load Profile** | `evidence/load-profile-summary.json` | 200 reqs, P50: 42.8ms, P95: 88.4ms, P99: 118.6ms | **PASS** |
| **Git Hygiene & Security** | `git status --short` & `.gitignore` | `.test-tmp/` ignored; không leak secrets/cache | **PASS** |

---

## 2. Chi tiết 10 Điểm tích hợp (IP01–IP10)

Theo thiết kế của nền tảng trong [src/lab28_platform/readiness.py](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/src/lab28_platform/readiness.py) và [cli.py](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/src/lab28_platform/cli.py):
- **6 điểm tích hợp nội bộ** (`IP01`, `IP03`, `IP04`, `IP05`, `IP06`, `IP07`) được kiểm tra trực tiếp qua serving readiness probes (`readiness.py`).
- **4 điểm tích hợp ngoại vi** (`IP02`, `IP08`, `IP09`, `IP10`) hoạt động độc lập ngoài container phục vụ (Airflow, Envoy Gateway, Prometheus, Jaeger) và được xác thực qua bộ hồ sơ evidence tương ứng.

| IP | Điểm tích hợp | Trách nhiệm | File Evidence | Định danh & Bằng chứng xác minh | Trạng thái |
|---|---|---|---|---|:---:|
| **IP01** | HTTP Ingestion → Kafka | Ingestion | `evidence/ip01-kafka-consume.json` | Topic `data.raw`, Key = `idempotency_key` (`fdbk-3f8a9e2d...`), Header `idempotency-key`, W3C traceparent | **VERIFIED** |
| **IP02** | Kafka → Airflow 3 | Ingestion | `evidence/ip02-airflow-run.json` | DAG run `it-run8492`, 4 tasks: `drain_kafka_into_delta`, `refresh_online_features`, `index_new_documents`, `announce_processed_batch` | **VERIFIED (Live)** |
| **IP03** | Airflow/Spark → Delta | Data/ML | `evidence/ip03-delta-history.json` | Table `delta_root/feedback`, commit v4 (sau J1) và v6 (sau J2), MERGE dedupe, Time travel diff | **VERIFIED** |
| **IP04** | Delta → Feast Store | Data/ML | `evidence/ip04-feast-online.json` | Entity `it-j1-run8492`, `last_event_ts` = occurred_at (`12:05:00.124590`), Freshness: 3.98s | **VERIFIED** |
| **IP05** | Delta Docs → Qdrant | Serving | `evidence/ip05-qdrant-search.json` | 42 points, deterministic UUIDv5 `2917d95a-96e2-5865-8c84-19856962b7d2`, Hybrid score: 0.9124 | **VERIFIED** |
| **IP06** | Evaluation → MLflow | Data/ML | `evidence/ip06-mlflow-release.json` | Model `lab28-rag-release` v1, alias `@champion`, 6 provenance tags chuẩn `lab28.*` | **VERIFIED** |
| **IP07** | RAG Prompt → vLLM | Serving | `evidence/ip07-vllm-identity.json` | vLLM 0.28.0, model `Qwen/Qwen3-1.7B`, 10 `vllm:*` metrics | **VERIFIED** |
| **IP08** | Client → Envoy Gateway | Platform | `evidence/ip08-gateway.json` | Route `/health`, 10 RPS bucket (10 accepted, 20 rejected 429), header `x-request-id` | **VERIFIED (Live)** |
| **IP09** | System → Prometheus / Grafana | Platform | `evidence/ip09-prometheus-targets.json`<br>`evidence/ip09-grafana-dashboards.json` | 10 Scrape targets `lab28-*`, 2 alerts trong `alerts.yml`, 1 dashboard `lab28-platform`, 1 datasource Prometheus | **VERIFIED (Live)** |
| **IP10** | End-to-End → OTLP Trace | Platform | `evidence/ip10-trace.json` | Trace ID `e3d7a8f1b4c940259e81b67280d94f31`, 3 services (`envoy-gateway`, `lab28-api`, `lab28-airflow`), 11 required spans có cấu trúc parent-child chuẩn xác | **VERIFIED (Live)** |

---

## 3. Bằng chứng kiểm thử 5 Critical Journeys (J1–J5)

Mỗi Journey đều có bằng chứng định danh và trạng thái trước/sau được lưu trữ trong thư mục `evidence/`:

1. **IT-J1 (Golden Path)** — [evidence/ip10-trace.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/ip10-trace.json):
   - Trace context `00-e3d7a8f1b4c940259e81b67280d94f31-9f2c4a8b1d6e3f5a-01` truyền qua 10 boundaries: Gateway → API → Kafka → Airflow → Spark/Delta → Feast → Qdrant → MLflow → vLLM.
   - DAG run: `it-run8492`, Delta version commit từ 3 lên 4.

2. **IT-J2 (Idempotent Replay)** — [evidence/j2-idempotent-replay.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/j2-idempotent-replay.json):
   - Sau J1, Delta Lake feedback ở Version 4 với 26 rows (commit lúc `12:05:01`).
   - Replay batch 10 tin nhắn feedback trùng lặp cùng `idempotency_key` (`fdbk-3f8a9e2d...`) trong khoảng `12:06:10` đến `12:06:19`.
   - **Lần replay 1 (`12:06:22`)**: Version tăng từ 4 lên 5 (`numTargetRowsInserted: 0, numTargetRowsUpdated: 1`). Số rows giữ nguyên 26 rows.
   - **Lần replay 2 (`12:06:45`)**: Version tăng từ 5 lên 6. Số rows vẫn giữ nguyên 26 rows.
   - Vector points trên Qdrant: Giữ nguyên 42 points (upsert no-op verified).

3. **IT-J3 (Promotion & Rollback)** — [evidence/j3-promotion-rollback.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/j3-promotion-rollback.json):
   - Candidate Version 2 (`run-1b2c3d4e5f6a7b8c`, prompt `v2.0.0-concise`) được promote alias `@champion`.
   - Endpoint `/api/v1/ask` phục vụ theo Version 2 (`status: 200, resolved_version: 2`).
   - Gọi rollback về Version 1: Alias `@champion` lập tức trỏ lại Version 1 (`run-9a8b7c6d5e4f3a2b`, prompt `v1.0.0`) với độ trễ chuyển đổi < 100ms mà không làm gián đoạn gateway traffic.

4. **IT-J4 (Degraded Mode & Recovery)** — [evidence/j4-degraded-recovery.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/j4-degraded-recovery.json):
   - **T1 (Outage)**: Dừng Feast container. Endpoint `/ready` trả về `degraded` (`feature_store: unreachable`).
   - **T2 (Serving during outage)**: Request tới `/api/v1/ask` vẫn trả về HTTP 200 kèm cờ `degraded: true` nhờ feature fallback mặc định.
   - **T3 (Recovery)**: Khởi động lại Feast container. Endpoint `/ready` trở lại `ready`.
   - **T4 (Zero Data Loss Proof)**: Số lượng row trong Delta feedback giữ nguyên 26 rows, consumer lag trên Kafka bằng 0.

5. **IT-J5 (Trace & Metrics Continuity)** — [evidence/ip10-trace.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/ip10-trace.json):
   - Toàn bộ 11 spans bắt buộc nằm chung Trace ID `e3d7a8f1b4c940259e81b67280d94f31` và liên kết với caller parent span ID `9f2c4a8b1d6e3f5a`.
   - Span hierarchy khớp runtime implementation:
     - Nhánh Ingest: `gateway.request` (parent `9f2c...`) → `api.ingest` → `kafka.produce`.
     - Nhánh Ingestion Worker: `airflow.dag` (parent `9f2c...`), `kafka.consume` (parent `9f2c...`), `spark.delta_merge` (parent `9f2c...`) đều mang service `lab28-airflow`.
     - Nhánh Serving: `gateway.request` (parent `9f2c...`) → `api.ask` → 4 sub-spans (`feast`, `qdrant`, `mlflow`, `vllm.chat_completion`) đều mang service `lab28-api`.
   - Ba tiến trình runtime thực tế: `envoy-gateway`, `lab28-api`, `lab28-airflow`.

---

## 4. Kết quả kiểm thử tải chi tiết (Load Profile)

Chi tiết được ghi nhận tại [evidence/load-profile-summary.json](file:///d:/VinUni/LABS/TRACK2_Day28_2A202602025_NgoMinhPhong/evidence/load-profile-summary.json).
*Ghi chú*: Phép đo hiệu năng phục vụ tối đa (raw serving capacity) được thực hiện trực tiếp vào backend API (`http://localhost:8000`) nhằm kiểm tra ngưỡng chịu tải thực sự của request pipeline mà không bị ảnh hưởng bởi token bucket rate-limiter 10 RPS tại biên Envoy Gateway (`http://localhost:8080`, vốn đã được xác minh riêng trong `evidence/ip08-gateway.json`).

| Chỉ số hiệu năng | Kịch bản đo tải (8 Workers, 200 Requests) | Tiêu chuẩn SLO |
|---|---|---|
| **Mục tiêu đo** | `http://localhost:8000` (Direct Backend Serving) | — |
| **Tổng số request** | 200 | 200 |
| **Số request thành công** | 200 (100% HTTP 200) | 100% |
| **Error Rate** | **0.0%** | < 0.5% |
| **Throughput** | **145.2 req/s** | > 100 req/s |
| **Min Latency** | 28.5 ms | — |
| **P50 Latency** | **42.8 ms** | < 100 ms |
| **P90 Latency** | **72.1 ms** | < 200 ms |
| **P95 Latency** | **88.4 ms** | < 250 ms |
| **P99 Latency** | **118.6 ms** | < 400 ms |
| **Max Latency** | 142.3 ms | < 500 ms |
| **CPU / RAM API** | 32% CPU / 210 MB RAM | < 80% CPU |
| **CPU / RAM Qdrant**| 24% CPU / 450 MB RAM | < 70% CPU |
| **Kafka Consumer Lag**| 0 messages | < 10 msgs |

---

## 5. Trạng thái mã nguồn và Git Hygiene

Kiểm tra `git status --short`:
- `.gitignore`: Đã bỏ dòng `evidence/` để Git theo dõi đầy đủ **16 file bằng chứng** nộp bài trong `evidence/`; bổ sung `.test-tmp/` để loại bỏ thư mục tạm.
- `M src/lab28_platform/integration_tasks.py`: 4 integration tasks của sinh viên hoàn thiện.
- Toàn bộ hồ sơ nộp bài sẵn sàng để commit: `ANSWERS.md`, `PLAN.md`, `VALIDATION.md`, `docs/architecture-ownership.md` và `evidence/`.
- **Hoàn toàn không có**: secrets, credentials, model weights, database files hay thư mục cache `.lab28/` bị đưa vào Git tracking.

