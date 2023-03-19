import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exam.oop.BackpressureConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class Main {

    private static final String TOPIC_NAME = "test-topic";
    private static final String CONSUMER_GROUP_NAME = "my_consumer_group";
    private static final String BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/bank";
    private static final String DB_USER = "consumer";
    private static final String DB_PASSWORD = "rlaehgud1!";

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    private static void processRecord(ConsumerRecord<String, String> record, Connection conn) throws SQLException {
        String sql = "INSERT INTO my_table (key, value) VALUES (?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, record.key());
            stmt.setString(2, record.value());
            stmt.executeUpdate();
        }
    }

    public static void main(String[] args) {
        BackpressureConsumer consumer = new BackpressureConsumer();
//        consumer.consume();



        KafkaConsumer<String, String> consumer = createConsumer();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap();

        try (Connection conn = getConnection()) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
//                    processRecord(record, conn);

                    // Store the current offset and metadata
                    offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }

                // Commit the offsets
                if (!records.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            }
        } catch (SQLException  e) {
            e.printStackTrace();
        } finally {
            // Commit the final offsets and close the consumer
            consumer.commitSync(offsets);
            consumer.close();
        }
    }


}
