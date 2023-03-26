
# Kafka Consumer Application

### Introduction
이 프로젝트는 대량의 Avro 형식의 메시지를 분산 병렬 처리하는 Java 기반 Kafka Consumer 애플리케이션입니다.
이 애플리케이션은 장애 복구 시에도 데이터 유실 없이 Exactly-Once Delivery를 보장합니다.
또한, Backpressure 기능을 지원하여 Consumer가 메시지를 처리하는 속도를 제어할 수 있습니다.


### Features
- Kafka Topic 멀티 파티션-스레드 대량의 메시지 분산 병렬 처리 
- 장애 복구 시 데이터 유실 없이 Exactly-Once Delivery 보장
- 블록킹 Queue 를 활용한 Backpressure 메시지 처리 속도 제어
- Avro 스키마 디시리얼라이즈, 동적으로 SQL 생성 및 데이터 삽입 

### Explanation
#### Kafka Topic 멀티 파티션-스레드 대량의 메시지 분산 병렬 처리
- 1개의 토픽의 N개의 파티션의 개수만큼 N개의 Thread 를 할당하여 메시지 분산 병렬 처리합니다.
- 1개의 토픽의 N개의 파티션을 컨슈밍하고 있는 Thread 들을 1개의 컨슈머 그룹으로 지정합니다.  
- Backpressure 기능을 위해 컨슈머 1개당 데이터베이스 처리하는 Thread 1개를 추가로 생성합니다. 
- 1개의 토픽 > N개의 파티션 > N개의 컨슈머 Thread > N개의 DB처리 Thread 로 구성합니다. 

#### 장애 복구 시 데이터 유실 없이 Exactly-Once Delivery 보장
- MySQL 데이터베이스에 Kafka 의 Consumer Offset 정보를 저장하여 관리합니다.
- 애플리케이션 정상/비정상 종료 후 재실행 시 MySQL 에서 Offset 을 읽어와서 메시지를 소비합니다.
- 1개의 파티션의 메시지를 처리하는 스레드를 1개로 제한하여 파티션의 메시지 처리 순서를 보장합니다.
- Kafka-MySQL 구간에서 장애가 발생하면 트랜잭션을 rollback 하여 Exactly-Once 를 보장합니다. 

#### 블록킹 Queue 를 활용한 Backpressure 메시지 처리 속도 제어
- Kafka 토픽을 구독하는 컨슈머 Thread 1개 당 DB처리 Thread 1개를 별도로 생성합니다.
- 토픽에서 컨슈밍한 메시지를 처리하기 전에 블록킹 Queue 에 추가하고 바로 다음 메시지를 컨슈밍합니다.
- DB처리 Thread 에서 메시지를 블록킹 Queue 에서 꺼내어 MySQL 에 처리하는 작업을 수행합니다. 
- 블록킹 Queue 사이즈를 초과하면 Backpressure 기능이 작동하여 메시지 처리 속도를 제어합니다. 
- 블록킹 Queue 개수가 줄어들 때까지 컨슈머 Thread 의 메시지 소비 작업을 대기하도록 합니다. 

#### Avro 스키마 디시리얼라이즈, 동적으로 SQL 생성 및 데이터 삽입
- Avro 스키마를 사용하여 Kafka 토픽에서 가져온 데이터를 Java 객체로 디시리얼라이즈합니다.
- 레코드 객체에 포함된 Avro 스키마를 사용하여 동적으로 SQL 문을 작성하고, MySQL 테이블에 저장합니다.
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

[INFO] --- compiler:3.1:compile (default-compile) @ consumer ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 4 source files to /Users/dohyung/ailab/realtime-pipeline/03-consumer/target/classes
[INFO] 
[INFO] --- exec:3.0.0:java (default-cli) @ consumer ---
[Main.main()] INFO Main - kafka consumer application start...

```

### Application Deploy
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 패키징합니다.
3. Java를 사용하여 백그라운드로 Consumer 애플리케이션을 실행합니다. 
```bash
$ mvn clean compile
$ nohup java -jar target/consumer-1.0-jar-with-dependencies.jar &

$ ps -ef |grep consumer 
  501 17491 10912   0  2:24PM ttys020    0:09.51 /usr/bin/java -jar target/consumer-1.0-jar-with-dependencies.jar
  501 17577 10912   0  2:25PM ttys020    0:00.00 grep consumer

```

### Application Stop
Mac이나 리눅스 기반의 OS에서는 Shell 파일을 이용해서 애플리케이션을 실행하거나 중지할 수 있습니다.
```bash
$ chmod 755 start.sh stop.sh
$ ./stop.sh

[2]  - killed     nohup java -jar target/consumer-1.0-jar-with-dependencies.jar
```

