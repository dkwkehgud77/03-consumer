package com.exam.worker;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AvroConsumer {
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");

    private static final Logger logger = LoggerFactory.getLogger(AvroConsumer.class);

    // Kafka Consumer 설정
    private final String topicName;
    private final int partitionId;
    private final int partitionCount;
    private final String consumerGroupId;
    private final KafkaConsumer<String, GenericData.Record> consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    // Backpressure 설정
    private final BlockingQueue<ConsumerRecord<String, GenericData.Record>> blockingQueue;
    private final int POLL_INTERVAL_MS;

    // MySQL 데이터베이스 작업
    public final MySQLConnectionPool dbPool;
    private final MySQLProcessData dbProcess;

    public AvroConsumer(String topicName, int partitionId, int partitionCount) {
        // Topic 과 Partition 를 기준으로 고유한 컨슈머 객체 1개 생성 -> 1개의 스레드에 할당
        // 1개의 Partition 을 N개의 컨슈머가 구독하여도 성능은 좋아지지 않는다 -> 병렬 처리하지 않음
        // 1개의 Topic 의 N개의 Partition 에 N개의 컨슈머가 구독하면 성능은 개선된다 -> 병렬 처리함
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.partitionCount = partitionCount;
        this.consumerGroupId = "group-" + topicName;

        // Backpressure 관련 설정
        int blockingQueueSize = Integer.parseInt(RESOURCE_BUNDLE.getString("blocking.queue.size"));
        int maxPollRecords = Integer.parseInt(RESOURCE_BUNDLE.getString("max.poll.records"));
        this.POLL_INTERVAL_MS = Integer.parseInt(RESOURCE_BUNDLE.getString("poll.interval.ms"));
        this.blockingQueue = new LinkedBlockingQueue<>(blockingQueueSize);

        // Kafka 컨슈머 프로퍼티 설정 -> Consumer Group 메시지 병렬 처리, Avro 스키마 디시리얼라이즈, Exactly Once 보장 관련 설정
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RESOURCE_BUNDLE.getString("kafka.bootstrap.servers"));   // Kafka Broker 연결
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);             // Topic 을 기준으로 컨슈머 그룹 설정 - 메시지 병렬 처리
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);    // 컨슈머가 한번에 가져올 수 있는 최대 레코드 수를 설정
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");           // 컨슈머가 수동으로 커밋을 수행하도록 설정 - Exactly Once 보장
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");         // 파티션 offset 데이터가 DB에 없으면 처음부터 메시지 소비
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());     // Key 디시리얼라이즈 설정 - Avro 스키마
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());   // Value 디시리얼라이즈 설정 - Avro 스키마
        props.put("schema.registry.url", RESOURCE_BUNDLE.getString("kafka.schema.registry.url"));      // 스키마 레지스트리 설정 - Avro 스키마의 호환성 보장, 버전 관리
        this.consumer = new KafkaConsumer<>(props);

        // MySQL 데이터베이스 작업 객체 및 Connection Pool 생성
        this.dbProcess = new MySQLProcessData();
        this.dbPool = dbProcess.createConnPool();
    }

    public void consumeMesseages() throws SQLException {

        // 컨슈머 에게 구독할 Topic 과 Partition 을 할당합니다.
        TopicPartition partition = new TopicPartition(topicName, partitionId);
        consumer.assign(Collections.singleton(partition));

        // 데이터베이스에서 컨슈머가 구독하고 있는 토픽-파티션의 Offset 값을 읽어옵니다.
        // 프로그램은 정상/비정상 종료 후 재실행 시 데이터 유실없이 메시지를 소비하기 위함입니다.
        Connection conn = dbPool.getConnection();
        currentOffsets = dbProcess.readOffsetsFromDB(conn, topicName, partitionId, consumerGroupId);


        // 읽어온 Offset 정보로 구독하는 Partition 에 대한 컨슈머의 메시지 소비 offset 을 설정합니다.
        // currentOffsets 값이 없다면 Partition 처음부터(earliest) 메시지를 읽어옵니다.
        if(!currentOffsets.isEmpty()){
            currentOffsets.forEach((tp, offset) -> consumer.seek(tp, offset.offset()));
        }


        while (true) {
            // 컨슈머가 계속해서 구독하고 있는 Partition 의 메시지를 소비합니다. 100밀리초 마다 데이터를 가져옵니다.
            ConsumerRecords<String, GenericData.Record> records = consumer.poll(Duration.ofMillis(POLL_INTERVAL_MS));

            for (ConsumerRecord<String, GenericData.Record> record : records) {
                // 메시지의 Key 값에 따라 동일한 Partition 에 잘 분배되어 들어가고 소비되는지 확인합니다. -> 해싱함수 사용
                // 동일한 Key 의 데이터는 동일한 Partition 에 저장되고 소비되어야 파티션 병렬 처리하여도 데이터의 순서가 보장됩니다.
                String key = record.key();
                int record_partition = Math.abs(key.hashCode() % partitionCount);
                logger.info(String.format("Thread %d received message from Topic %s partition %d", partitionId, topicName, record_partition));

                try {
                    // Backpressure , 컨슈머가 소비한 메시지를 MySQL 테이블에 삽입합니다.
                    boolean isOffered = blockingQueue.offer(record, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    if (!isOffered) {
                        // blockingQueue 가 가득차면 Backpressure 가 작동합니다.
                        handleBackpressure(record);
                    }

                    // 컨슈머가 처리한 Record 와 Offset 을 MySQL 데이터베이스에 저장합니다.
                    processRecordAndOffset(record);

                } catch (InterruptedException | SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            }

            // 주기적으로 offset 정보를 외부 저장소(MySQL)에 저장합니다.
            dbProcess.saveOffsetToDB(conn, currentOffsets, consumerGroupId);
        }

    }

    private void handleBackpressure(ConsumerRecord<String, GenericData.Record> record) throws InterruptedException, SQLException {

        // blockingQueue 가 가득 찬 경우, backpressume 가 해소될 때 까지 일정시간 대기 합니다.
        while (!blockingQueue.offer(record, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
            Thread.sleep(1000); // 일정 시간 대기
            boolean isOffered = blockingQueue.offer(record);
            if (isOffered) {
                logger.warn("Backpressure has been resolved. Resuming message processing.");
                return;
            }
        }

    }

    private void processRecordAndOffset(ConsumerRecord<String, GenericData.Record> record) throws SQLException {

        // Connection Pool 에서 커넥션 가져오고, autocommit 모드 비활성화 합니다.
        Connection conn = dbProcess.getConnMySQL(dbPool);
        conn.setAutoCommit(false);
        try {

            // 해당 Record 의 Avro Schema 에 포함된 테이블 포맷에 맞게 동적으로 삽입 합니다.
            dbProcess.insertRecordToDB(conn, record);

            // 현재 consumer에 대한 offset 정보를 업데이트 합니다  -> +1 해줘야하는 건지?
            currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

            // 처리한 offset 정보를 데이터베이스 kafka_offset 테이블에 업데이트 합니다.
            dbProcess.saveOffsetToDB(conn, currentOffsets, consumerGroupId);

            // Record 와 Offset 모두 정상적으로 DB에 업데이트 되면 커밋합니다.
            conn.commit();

        } catch (Exception e) {
            e.printStackTrace();
            try {
                // Exception 발생시 해당 커넥션 작업 롤백합니다. -> Exactly Once 보장
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            consumer.close();
        } finally {
            conn.close();
            // 작업이 끝나면 MySQL 커넥션을 Connection Pool 에 반환합니다.
            dbPool.returnConnection(conn);
        }
    }

}



