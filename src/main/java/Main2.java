import com.exam.worker.AvroConsumer;
import com.exam.worker.AvroProducer;
import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Main2 {
    private static final Logger logger = LoggerFactory.getLogger(Main2.class);

    private static final int THREAD_COUNT = 1;

    public static void main(String[] args) throws IOException {
        // 첨부된 스키마를 내용으로 JSON 파일 읽음
        String jsonFilePath = "./src/main/resources/schema/schema_avro.json";
        List<Schema> avroSchemaList = new ArrayList<>();

        try(FileReader reader = new FileReader(jsonFilePath)){
            // Json 파일 읽기
            JSONParser parser = new JSONParser();
            JSONArray avroJsonArray = (JSONArray) parser.parse(reader);
            logger.info(avroJsonArray.toJSONString());

            // Avro 스키마 생성
            for(Object obj : avroJsonArray) {
                JSONObject avroSchemaJson = (JSONObject) obj;
                Schema avroSchema = new Schema.Parser().parse(avroSchemaJson.toJSONString());
                avroSchemaList.add(avroSchema);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }


        for(Schema avroSchema : avroSchemaList) {
            // 스레드 풀 생성, 병렬로 메시지 생산
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                int threadId = i;
                executor.submit(() -> {
                    AvroProducer producer = new AvroProducer();
                    producer.createMessages(threadId, avroSchema);
                });
            }

            // 메시지 생산이 완료되면 스레드 종료
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                executor.shutdownNow();
            }

        }

    }

}
