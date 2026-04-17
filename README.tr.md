# OmniNOC Fault Management Platform

[English Version (README.md)](README.md)

Bu proje, alarm toplama (ingestion) ve ilişkilendirme (correlation) süreçleri için geliştirdiğim event-driven bir mikroservis backend uygulaması. Temel amacı, gerçek bir Ağ Operasyon Merkezi'nin (NOC) binlerce anlamsız alarmı nasıl anlamlı olaylara (incident) dönüştürdüğünü simüle etmek.

## Çözdüğümüz Problem
Düşünün, tek bir fiber kablo koptuğunda taşıma, erişim ve servis katmanlarında yüzlerce farklı alarm çalar. Eğer bu alarmları birbiriyle ilişkilendirmezseniz, NOC operatörleri ekrana düşen alarm fırtınasında boğulur.

**Hedef:** Aynı kök nedenden (root cause) kaynaklanan alarmları tek bir olay altında gruplamak ve otomatik olarak doğru müdahale ekibine atamak.

## Sistem Nasıl Çalışıyor?
```
OSS/EMS/NMS  --POST-->  alarm-ingestion-service  --Kafka-->  correlation-engine
                             (port 8081)           alarm.normalized   (port 8082)
                                                                          |
                                                               Redis (sliding window)
                                                               PostgreSQL (incidents)
                                                               Kafka (incident.updated)
```

- **alarm-ingestion-service:** Dış sistemlerden REST üzerinden ham alarmları alır, standart bir formata çevirir ve Kafka'ya fırlatır.
- **correlation-engine:** Kafka'dan bu alarmları okur, korelasyon kurallarından geçirir ve Postgres üzerinde yeni incident oluşturur veya var olanı günceller.

## Kullandığım Teknolojiler
- **Java 21 & Spring Boot 3.3**
- **PostgreSQL 16 & Flyway** (Kalıcı veri ve veritabanı versiyonlama)
- **Apache Kafka 3.7** (Asenkron mesajlaşma kuyruğu)
- **Redis 7** (Her saha için 10 dakikalık zaman pencerelerini tutmak için)
- **Docker Compose** (Tüm altyapıyı tek komutla ayağa kaldırmak için)

## Korelasyon Kuralları
Engine içine 4 temel kural yazdım:
1. **Zaman ve Saha (Site) Penceresi:** Aynı sahadan (siteId) 10 dakika içinde gelen alarmlar tek bir incident altında toplanır. Bunu Redis'in TTL özelliğiyle çok temiz bir şekilde çözdüm.
2. **Önem Derecesi (Severity) Artırma:** Eğer alarm bir `CORE_ROUTER` cihazından geliyorsa, sistem otomatik olarak alarmın seviyesini bir tık yukarı çeker.
3. **Parent-Child Algılaması:** Bir `FIBER_CUT` alarmı yeni bir incident açar. Hemen ardından aynı sahadan `SERVICE_DEGRADED` gelirse, sistem bunun fiber kesintisinden kaynaklandığını anlar ve `CHILD` alarm olarak işaretler.
4. **Kök Neden Kararı:** İlk gelen kritik alarma bakarak sorunun ana kaynağı ve hangi ekibin (Field Ops, Transmission vs.) ilgilenmesi gerektiği otomatik belirlenir.

## Nasıl Çalıştırılır?

Kafka, Redis ve Postgres altyapısını docker-compose ile ayağa kaldırın:
```bash
cd infra/docker-compose
docker-compose up -d
```

Uygulamalar ayağa kalktıktan sonra API'lara buradan ulaşabilirsiniz:
- Ingestion API: `http://localhost:8081/swagger-ui.html`
- Correlation API: `http://localhost:8082/swagger-ui.html`

## Test Edelim (Demo)

Sistemi test etmek için terminalden bir fiber kesintisi senaryosu başlatabilirsiniz. Önce kök nedeni, sonra da ona bağlı ikincil alarmı gönderin:

```bash
# 1. Kök Neden (Fiber Kesintisi)
curl -X POST http://localhost:8081/api/v1/alarms/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sourceSystem": "OSS",
    "alarmType": "FIBER_CUT",
    "severity": "CRITICAL",
    "siteId": "IST-TRX-001",
    "region": "Istanbul",
    "deviceType": "TRANSMISSION_NODE",
    "deviceId": "IST-TRX-001",
    "description": "Ana hatta fiber kesintisi"
  }'

# 2. İkincil Etki (Linkin Düşmesi)
curl -X POST http://localhost:8081/api/v1/alarms/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sourceSystem": "NMS",
    "alarmType": "LINK_DOWN",
    "severity": "MAJOR",
    "siteId": "IST-TRX-001",
    "region": "Istanbul",
    "deviceType": "TRANSMISSION_NODE",
    "deviceId": "IST-TRX-001",
    "description": "Fiber kesintisi sonrasi link koptu"
  }'
```
Daha sonra `http://localhost:8082/api/v1/correlation/stats` adresine giderek istatistikleri kontrol edebilirsiniz. Sistemin 2 alarmı işlediğini ama sadece 1 incident oluşturduğunu göreceksiniz.

## Bazı Tasarım Kararları
- **Neden Kafka?** Alarm toplama hızıyla, işleme hızını birbirinden ayırmak için. Eğer devasa bir alarm fırtınası koparsa Kafka bunu tamponlar ve correlation engine kendi hızında çalışmaya devam eder.
- **Neden mesajları siteId'ye göre grupladım?** Kafka'daki partition yapısını `siteId` üzerinden kurdum. Böylece aynı sahaya ait alarmlar her zaman aynı consumer thread'ine düşer ve sıralı işlenme garantisi sağlanır.
- **Neden Redis TTL?** Eski alarm pencerelerini temizlemek için cron job yazmak yerine Redis'in 10 dakikalık TTL (Time To Live) özelliğini kullandım, süre dolunca otomatik siliniyor.
