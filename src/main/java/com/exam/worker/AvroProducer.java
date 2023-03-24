package com.exam.worker;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class AvroProducer {
    private static final Logger logger = LoggerFactory.getLogger(AvroProducer.class);

    private final String BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093";
    private final String TOPIC = "test_topic";
    private static final int MESSAGE_COUNT = 100;  // 생성할 메시지 수
    private static final int PARTITION_COUNT = 3;  // 토픽 파티션 수

    private static final Random RANDOM = new Random();

    private KafkaProducer<String, GenericRecord> producer;

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public AvroProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        this.producer = producer;
    }

    public void createMessages(int ThreadId, Schema schema) {
        try{
            String topic = schema.getName();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                GenericRecord message = new GenericData.Record(schema);
                for(Schema.Field field : schema.getFields()){
                    message.put(field.name(), generateValue(field.schema()));
                }
                String key = message.get(0).toString();
                int partition = Math.abs(key.hashCode() % PARTITION_COUNT);
                ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, partition, key, message);
                producer.send(producerRecord);
            }
            System.out.println("Thread "+ ThreadId + " Kafka Producer messeage send successfully..." + topic);

            producer.flush();
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object generateValue(Schema fieldSchema) {
        switch (fieldSchema.getType()) {
            case INT:
                return RANDOM.nextInt();
            case LONG:
                return System.currentTimeMillis();
            case FLOAT:
                return RANDOM.nextFloat();
            case DOUBLE:
                return RANDOM.nextDouble();
            case STRING:
//                byte[] bytes = new byte[16];
//                RANDOM.nextBytes(bytes);
//                return new String(bytes);
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < 6; i++) {
                    int index = new Random().nextInt(CHARACTERS.length());
                    char ch = CHARACTERS.charAt(index);
                    builder.append(ch);
                }
                return builder.toString();

            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldSchema.getType());
        }
    }

//    public void createMessages(Schema avroSchema) {
//        List<GenericRecord> messages = new ArrayList<>();
//
//        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(avroSchema);
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//
//        for (int i = 0; i < MESSAGE_COUNT; i++) {
//            GenericRecord message = new GenericData.Record(schema);
//            message.put("id", i);
//            message.put("name", "Name_" + i);
//            message.put("age", RANDOM.nextInt(100));
//            datumWriter.write(message, encoder);
//            encoder.flush();
//            byte[] serializedMessage = outputStream.toByteArray();
//            messages.add(deserializeMessage(serializedMessage, schema));
//            outputStream.reset();
//        }
//    }
}
