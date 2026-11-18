# TradeStream — Ingestor

**TR** | [EN](#en)

---

## TR

Gerçek zamanlı piyasa verisi işleme servisi. Simüle edilen fiyat verileri Kafka'ya yazılır, Kafka Streams ile sembol bazında 30 saniyelik pencerelerde ortalaması hesaplanır ve sonuçlar WebSocket (STOMP) üzerinden canlı bir dashboard'a iletilir.

### Mimari

```
MarketSimulationService ──┐
  (her 1 sn rastgele fiyat) │
                            ├──► Kafka topic: market-data
GET /api/send ─────────────┘            │
                                        ├──► KafkaConsumerService ──► /topic/market  (ham veri)
                                        │                        └──► /topic/alerts  (fiyat < 20.000$ alarmı)
                                        │
                                        └──► TradeAnalyticsStream (Kafka Streams)
                                               groupBy(symbol) → 30 sn tumbling window → ortalama
                                               └──► /topic/analytics
                                                          │
                                         WebSocket /ws (SockJS + STOMP)
                                                          │
                                               index.html dashboard
```

### Bileşenler

| Bileşen | Görev |
|---------|-------|
| `MarketSimulationService` | `@Scheduled` ile saniyede bir BTC / ETH / SOL / AVAX için rastgele fiyat üretir; ~%10 ihtimalle düşük fiyat üreterek alarm akışını tetikler. |
| `MarketDataProducer` / `KafkaMarketDataDataProducer` | Üretici soyutlaması; mesajları sembol key'iyle `market-data` topic'ine yazar (aynı sembol aynı partition'a düşer). |
| `KafkaConsumerService` | `@KafkaListener` ile ham veriyi tüketir, dashboard'a iletir ve eşik altındaki fiyatlar için alarm yayınlar. |
| `TradeAnalyticsStream` | Kafka Streams topolojisi: JSON parse → sembole göre gruplama → 30 sn'lik pencerede toplam/adet aggregate → ortalama. |
| `WebSocketConfig` | `/ws` STOMP endpoint'i ve `/topic/*` basit broker'ı. |
| `TestController` | `GET /api/send?symbol=BTC&price=15000` ile elle mesaj gönderme. |

### Teknolojiler

| Katman | Teknoloji |
|--------|-----------|
| Backend | Java 21, Spring Boot 4 |
| Mesajlaşma | Apache Kafka, Spring Kafka |
| Stream işleme | Kafka Streams (windowed aggregation) |
| Gerçek zamanlı iletim | WebSocket, STOMP, SockJS |
| Konteyner | Docker (multi-stage build), Docker Compose |
| CI | GitHub Actions (Kafka ayağa kaldırılıp `mvn verify`) |
| Deploy | Daha önce AWS ECS üzerinde çalıştırıldı (şu an aktif değil) |

### Çalıştırma

**Altyapı (Kafka, Zookeeper, Kafka UI):**
```bash
docker compose up -d zookeeper kafka kafka-ui
```

**Uygulama:**
```bash
./mvnw spring-boot:run
```

- `http://localhost:8081` — canlı dashboard
- `http://localhost:8080` — Kafka UI
- `http://localhost:8081/api/send?symbol=BTC&price=15000` — elle mesaj (alarm tetikler)

---

## EN
<a name="en"></a>

Real-time market data processing service. Simulated price ticks are published to Kafka, averaged per symbol over 30-second windows with Kafka Streams, and pushed to a live dashboard over WebSocket (STOMP).

### Architecture

```
MarketSimulationService ──┐
  (random tick every 1s)    │
                            ├──► Kafka topic: market-data
GET /api/send ─────────────┘            │
                                        ├──► KafkaConsumerService ──► /topic/market  (raw ticks)
                                        │                        └──► /topic/alerts  (price < $20,000)
                                        │
                                        └──► TradeAnalyticsStream (Kafka Streams)
                                               groupBy(symbol) → 30s tumbling window → average
                                               └──► /topic/analytics
                                                          │
                                         WebSocket /ws (SockJS + STOMP)
                                                          │
                                               index.html dashboard
```

### Components

| Component | Responsibility |
|-----------|----------------|
| `MarketSimulationService` | Generates a random price for BTC / ETH / SOL / AVAX every second via `@Scheduled`; ~10% of ticks are low prices to exercise the alert path. |
| `MarketDataProducer` / `KafkaMarketDataDataProducer` | Producer abstraction; writes to `market-data` keyed by symbol (same symbol → same partition). |
| `KafkaConsumerService` | `@KafkaListener` that forwards raw ticks to the dashboard and publishes alerts below the threshold. |
| `TradeAnalyticsStream` | Kafka Streams topology: JSON parse → group by symbol → 30s windowed sum/count aggregate → average. |
| `WebSocketConfig` | `/ws` STOMP endpoint and simple `/topic/*` broker. |
| `TestController` | Manual publish via `GET /api/send?symbol=BTC&price=15000`. |

### Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Java 21, Spring Boot 4 |
| Messaging | Apache Kafka, Spring Kafka |
| Stream processing | Kafka Streams (windowed aggregation) |
| Real-time delivery | WebSocket, STOMP, SockJS |
| Container | Docker (multi-stage build), Docker Compose |
| CI | GitHub Actions (spins up Kafka, runs `mvn verify`) |
| Deployment | Previously ran on AWS ECS (not currently live) |

### Running

**Infrastructure (Kafka, Zookeeper, Kafka UI):**
```bash
docker compose up -d zookeeper kafka kafka-ui
```

**Application:**
```bash
./mvnw spring-boot:run
```

- `http://localhost:8081` — live dashboard
- `http://localhost:8080` — Kafka UI
- `http://localhost:8081/api/send?symbol=BTC&price=15000` — manual tick (triggers an alert)
