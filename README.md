
# 컨슈머

### 1. infra 세팅
- Zookeeper, Kafka, Schema-Registry, MySQL 생성
- AVRO 스키마, Kafka 토픽, MySql 테이블 생성

```bash
cd pipeline/01-infra
docker-compose up -d 

mvn clean package
mvn exec:java
# java -jar infra-1.0-SNAPSHOT-jar
```


pkill -9 -f /Users/dohyung/ailab/realtime-pipeline