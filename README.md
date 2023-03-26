
# Kafka Consumer Application

## Introduction
이 프로젝트는 대량의 Avro 형식의 메시지를 분산 병렬 처리하는 Java 기반 Kafka Consumer 애플리케이션입니다.
이 애플리케이션은 장애 복구 시에도 데이터 유실 없이 Exactly-Once Delivery를 보장합니다.
또한, Backpressure 기능을 지원하여 Consumer가 메시지를 처리하는 속도를 제어할 수 있습니다.


## Features
#### Kafka 대량의 메시지 분산 병렬 처리
- Kafka 토픽에 Partition의 개수만큼 Thread를 할당하여 메시지 분산 병렬 처리합니다.
- 토픽 Partition 을 구독하고 있는 Thread를 컨슈머 Group 으로 묶어서 컨슈밍합니다. 
- Thread에서 MySQL DB에 적재되는 작업은 비동기로 처리하여 처리 속도를 향상시킵니다. 

#### 데이터 유실 없이 Exactly-Once Delivery 보장
- MySQL 데이터 베이스에 Kafka의 Consumer Offset 정보를 저장하여 관리합니다.
- Consumer 애플리케이션 재실행 시 MySQL에서 Offset을 읽어와서 메시지를 소비합니다.
- Kafka-MySQL 구간에서 장애가 발생하면 트랜잭션을 rollback 처리하여 Exactly-Once를 보장합니다. 

#### Backpressure 기능으로 메시지 처리하는 속도 제어
- Kafka에서 컨슈밍한 메시지를 처리하기 전에 blocking queue 에 담습니다.
- blocking queue 에서 꺼내어 MySQL DB에 적재되는 작업은 비동기로 처리합니다.
- blocking queue 사이즈를 제한해두고 초과하면 Backpressure 가 작동합니다.
- blocking queue 개수가 줄어들 때까지 Thread의 메시지 처리 작업을 대기하도록 합니다. 

#### Avro 스키마 레지스트리를 활용한 메시지 디시리얼라이즈
- Avro 스키마를 사용하여 Kafka Topic에서 가져온 데이터를 자바 객체로 디시리얼라이즈합니다.
- Avro 스키마 레지스트리를 활용하여 스키마 및 버전을 관리하여 데이터 형식 변경에 유연하게 대처합니다. 
 

# Getting Started
### Prerequisites 
- Java JDK 11
- Apache Maven 3.9.0
- Apache Kafka 2.8.1
- MySQL 8.0

### Configuration
프로퍼티 파일에 Kafka Consumer 애플리케이션에 필요한 구성 설정을 합니다. 
```properties
# Kafka
kafka.bootstrap.servers=localhost:9091,localhost:9092,localhost:9093
kafka.topics=dataset1,dataset2,dataset3
kafka.schema.registry.url=http://localhost:8081

# Back Pressure
max.poll.records=10
poll.interval.ms=100
blocking.queue.size=300

# MySQL
mysql.url=jdbc:mysql://localhost:3306/bank
mysql.username=consumer
mysql.password=consumer1!
mysql.maxPoolSize=50
```

### Application Start
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 빌드합니다.
3. Maven을 사용하여 Consumer 애플리케이션을 실행합니다. 
```bash
$ mvn clean compile
$ mvn exec:java
```

### Application Deployment
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 패키징합니다.
3. Java를 사용하여 백그라운드로 Consumer 애플리케이션을 실행합니다. 
```bash
$ mvn clean compile
$ nohup java -jar target/consumer-1.0-jar-with-dependencies.jar &
```

### Application Stop
Mac이나 리눅스 기반의 OS에서는 Shell 파일을 이용해서 애플리케이션을 실행하거나 중지할 수 있습니다.
```bash
$ chmod 755 start.sh stop.sh
$ ./stop.sh
```

