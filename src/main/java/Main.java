import com.exam.worker.AvroConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        // AdminClient 설정을 구성하고, config.properties 에서 컨슈밍 할 Kafka 토픽을 가져옵니다.
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, RESOURCE_BUNDLE.getString("kafka.bootstrap.servers"));
        String[] topics = RESOURCE_BUNDLE.getString("kafka.topics").split(",");
        Map<String, Integer> topicPartitions  = new HashMap<>();

        int nThreads = 0;
        // AdminClient 를 통해 Topic 명으로 Partition 개수 를 구해서 Map 자료구조에 담습니다.
        try (AdminClient adminClient = AdminClient.create(properties)) {
            for(String topicName: topics) {
                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
                int partitionCount = describeTopicsResult.all().get().get(topicName).partitions().size();
                topicPartitions.put(topicName, partitionCount);
                // 각 Partition 개수를 합하여 스레드 풀 내부에서 생성될 스레드의 개수를 구합니다.
                nThreads += partitionCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // 모든 Topic 의 Partition 개수를 합한 만큼 스레드를 생성하는 Thread Pool 을 구성합니다.
        ExecutorService executor_topic = Executors.newFixedThreadPool(nThreads);
        for (Map.Entry<String, Integer> entry : topicPartitions.entrySet()) {
            String topicName = entry.getKey();
            int partitionCount = entry.getValue();

            // 각 Topic 의 Partition 개수 만큼 loop 를 돌아 각 스레드에 할당하여...
            for (int i = 0; i < partitionCount; i++) {
                int partitionId = i;
                // 각 스레드 에서 AvroConsumer 객체를 생성하고 메시지를 소비를 시작합니다.
                executor_topic.execute(() -> {
                    try {
                        AvroConsumer consumer = new AvroConsumer(topicName, partitionId, partitionCount);
                        consumer.consumeMesseages();
                    } catch (SQLException e){
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } catch (Exception e){
                        e.printStackTrace();
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