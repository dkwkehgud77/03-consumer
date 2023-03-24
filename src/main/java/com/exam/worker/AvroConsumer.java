package com.exam.worker;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AvroConsumer {
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");

    private static final Logger logger = LoggerFactory.getLogger(AvroConsumer.class);

    private Properties props;

    private Schema.Parser parser;

    private String topicName;
    private int partitionId;

    private KafkaConsumer<String, GenericData.Record> consumer;
//    private KafkaProducer<String, String> producer;

    private final int NUM_THREADS_PER_TOPIC = 3;
    private final long POLL_INTERVAL_MS = 1000;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private static final int MAX_RECORDS = 100;

    public MySQLConnectionPool dbPool = null;
    private MySQLProcessData dbProcess = null;

    public Connection getConnMySQL() throws SQLException {
        Connection conn = null;
        if(dbPool == null){
            try{
                // FlatMapFunction 안에서 MySQL 드라이버를 찾기위해 아래의 로직이 필요합니다.
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            // MySQLConnectionPool 객체 생성
//            dbPool = new MySQLConnectionPool(
//                    RESOURCE_BUNDLE.getString("aurora.url")+"/Hlms?useSSL=false",
//                    RESOURCE_BUNDLE.getString("aurora.username"),
//                    RESOURCE_BUNDLE.getString("aurora.password"),70);
        }
        try {
            // MySQLConnectionPool 에서 커넥션 pool을 가져옵니다.
            conn = dbPool.getConnection();
//            logger.info("occupiedPool size : " + String.valueOf(dbPool.occupiedPool.size()));
//            logger.info("freePool size : " + String.valueOf(dbPool.freePool.size()));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return conn;
    }

    public AvroConsumer(String topicName, int partitionId) throws SQLException {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.parser = new Schema.Parser();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-"+topicName);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

//        this.producer = new KafkaProducer<>(props);
        this.consumer = new KafkaConsumer<>(props);
        this.props = props;
        this.dbProcess = new MySQLProcessData();
        this.dbPool = dbProcess.createConnPool();
    }

    public void consumeMesseages() throws SQLException {

        TopicPartition partition = new TopicPartition(topicName, partitionId);
        consumer.assign(Collections.singleton(partition));

        // Consumer가 시작하는 offset을 DB에서 읽어들입니다.
        Connection conn = dbPool.getConnection();
        currentOffsets = dbProcess.readOffsetsFromDB(conn, topicName, partitionId);

        // 읽어온 offset 정보로 Consumer의 시작 offset을 설정합니다.
        // currentOffsets 값이 없다면 Partition 처음부터 메시지를 읽어옵니다.
        if(!currentOffsets.isEmpty()){
            currentOffsets.forEach((tp, offset) -> consumer.seek(tp, offset.offset()));
        }

        while (true) {
            ConsumerRecords<String, GenericData.Record> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, GenericData.Record> record : records) {
                System.out.println("record ::" + record.toString());
                String key = record.key();
                int record_partition = Math.abs(key.hashCode() % NUM_THREADS_PER_TOPIC);
                if (record_partition == partitionId) {
                    // Process the record
                    String recordLog = String.format("Thread %d received message from Topic %s partition %d", partitionId, topicName, record_partition );
                    System.out.println(recordLog);

                    try {
                        processRecord(record);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

                // 처리한 offset 정보를 DB에 저장합니다.
                dbProcess.saveOffsetToDB(conn, currentOffsets);
                consumer.commitSync();
            }
            currentOffsets.forEach((tp, offset) -> consumer.commitSync(Collections.singletonMap(tp, offset)));
            currentOffsets.clear();
        }

    }

    private void processRecord(ConsumerRecord<String, GenericData.Record> record) throws SQLException {
        // Backpressure를 적용하기 위한 로직
        if (currentOffsets.size() >= MAX_RECORDS) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            processRecord(record);
            return;
        }

//        currentOffsets.forEach((partition, offset) -> consumer.commitSync(Collections.singletonMap(partition, offset)));
//        currentOffsets.clear();
        try{
            messageInsert(record);
        }catch (Exception e){
            e.printStackTrace();
        }

        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }

    private void messageInsert(ConsumerRecord<String, GenericData.Record> record) throws SQLException {
        // Kafka Producer 설정
        Connection conn = dbProcess.getConnMySQL(dbPool);
        conn.setAutoCommit(false); // autocommit 모드 비활성화
        try {
            while (true) {
//                producer.beginTransaction();

//                String fields = record.value();
//                System.out.println("fields :: " + fields);

                GenericData.Record recordValue =  record.value();
                Schema schema = recordValue.getSchema();


                List<Schema.Field> fields = schema.getFields();
                // 필드 이름을 추출하여 문자열 리스트로 변환합니다.
                List<String> fieldNames = fields.stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                String fieldString = String.join(", ", fieldNames);

                // `?`를 필드명 개수만큼 반복해서 문자열을 구성합니다.
                String placeholders = fieldNames.stream().map(s -> "?").collect(Collectors.joining(", "));

                String insert_sql = String.format("INSERT INTO %s (%s) VALUES (%s)", topicName, fieldString, placeholders);
                System.out.println(insert_sql);
                PreparedStatement stmt = conn.prepareStatement(insert_sql);

                // MySQL Insert 처리
                for( int i=0; i < fields.size(); i++){
                    int idx = i+1;
                    Schema.Field field = fields.get(i);
                    String fieldName = field.name();
                    String fieldType = field.schema().getType().name();
                    Object value = recordValue.get(fieldName);
                    stmt = dbProcess.setParameter(stmt, idx, value, fieldType);
                }

                int result = stmt.executeUpdate();
                // 실행 결과를 확인합니다.
                if (result > 0) {
                    System.out.println("SQL statement executed successfully. " + result + " row(s) affected.");
                } else if (result == 0) {
                    System.out.println("SQL statement executed successfully, but no rows were affected.");
                } else {
                    System.out.println("SQL statement execution failed.");
                }

//                stmt.setInt(1, Integer.parseInt(record.value()));
//                stmt.setString(2, record.value());
//                stmt.executeUpdate();
//                stmt.addBatch();

                // 결과 출력
//                System.out.println(counts + " row(s) affected");

//                producer.commitTransaction();
                consumer.commitSync();
                conn.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
//            producer.abortTransaction();
//            consumer.close();
            if (conn != null) {
                try {
                    conn.rollback(); // 롤백
                } catch (SQLException ex) {
                    // 롤백 실패 처리
                }
            }
        } finally {
//            producer.close();
//            consumer.close();
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // autocommit 모드 활성화
                    conn.close(); // 커넥션 닫기
                    dbPool.returnConnection(conn);
                } catch (SQLException e) {
                    // 커넥션 닫기 실패 처리
                }
            }
        }
    }
}



//else {
//        Set<TopicPartition> topicPartitions = consumer.assignment();
//        consumer.seekToBeginning(topicPartitions);
//        }