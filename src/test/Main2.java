import com.exam.oop.BackpressureConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class AvroProducer {
    private static final String TOPIC_NAME = "test-topic";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String SCHEMA_STRING = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"id\", \"type\":\"int\" },"
            + "  { \"name\":\"name\", \"type\":\"string\" }"
            + "]}";

    public static void main(String[] args) {

        try {
            // JSON 파일 경로 설정
            String jsonFilePath = "path/to/schema_before.json";
            File jsonFile = new File(jsonFilePath);

            // JSON 파일에서 스키마 파싱
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(jsonFile);

            System.out.println(schema.toString(true)); // 생성된 스키마 출력

            // KafkaProducer 설정
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

            // KafkaProducer 객체 생성
            Producer<Object, Object> producer = new KafkaProducer<>(props);

            // Avro 메시지 생성
            GenericRecord avroMsg = new GenericData.Record(schema);
            avroMsg.put("id", 1);
            avroMsg.put("name", "John");

            // ProducerRecord 생성 및 KafkaProducer로 메시지 전송
            ProducerRecord<Object, Object> record = new ProducerRecord<>(TOPIC_NAME, avroMsg);
            producer.send(record);

            // KafkaProducer 종료
            producer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
