import com.exam.worker.AvroConsumer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main3 {

    private static final int THREAD_COUNT_PER_TOPIC = 3;

    public static void main(String[] args) throws IOException {

        List<String> TopicList = Arrays.asList("dataset1", "dataset2", "dataset3");
        for(String topicName : TopicList) {
            // 스레드 풀 생성, 병렬로 메시지 소비
            ExecutorService executor_topic = Executors.newFixedThreadPool(THREAD_COUNT_PER_TOPIC);
            for (int i = 0; i < THREAD_COUNT_PER_TOPIC; i++) {
                int partitionId = i;
                executor_topic.execute(() -> {
                    try {
                        AvroConsumer consumer = new AvroConsumer(topicName, partitionId);
                        consumer.consumeMesseages();

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }
}




//    Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");
//                props.put("group.id", "my-group");
//                props.put("auto.offset.reset", "earliest");
//                props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//                props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//                props.put("schema.registry.url", "http://localhost:8081");

//        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("dataset1", "dataset2", "dataset3"));


//        while (true) {
//            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
//            for (ConsumerRecord<String, GenericRecord> record : records) {
//                System.out.printf("key = %s, value = %s%n", record.key(), record.value());
//            }
//        }