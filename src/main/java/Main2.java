import com.exam.work.Producer;
import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main2 {
    public static void main(String[] args) throws IOException {
        // 첨부된 스키마를 내용으로 JSON 파일 읽음
        String jsonFilePath = "./src/main/resources/schema/schema_avro.json";
        try(FileReader reader = new FileReader(jsonFilePath)){
            JSONParser parser = new JSONParser();
            JSONArray avroJsonArray = (JSONArray) parser.parse(reader);
            System.out.println(avroJsonArray.toJSONString());

            for(Object obj : avroJsonArray) {
                JSONObject avroSchemaJson = (JSONObject) obj;
                // Avro 스키마로 생성되는지 확인
                Schema avroSchema = new Schema.Parser().parse(avroSchemaJson.toJSONString());
                Producer producer = new Producer();
                producer.createMessages(avroSchema);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

    }

}
