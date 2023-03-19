import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.exam.work.SetPipeline;
import org.apache.avro.Schema;


import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Main1 {
    public static void main(String[] args) throws IOException {
        SetPipeline pipeline = new SetPipeline();

        // 첨부된 스키마를 내용으로 JSON 파일 읽음
        String jsonFilePath = "./src/main/resources/schema/schema_before.json";
        try(FileReader reader = new FileReader(jsonFilePath)){
            JSONParser parser = new JSONParser();
            JSONArray beforeJsonArray = (JSONArray) parser.parse(reader);
            System.out.println(beforeJsonArray.toJSONString());

            List<Schema> avroSchemaList = pipeline.createSchema(beforeJsonArray);
            pipeline.createTopic(avroSchemaList);
            pipeline.createTable(avroSchemaList);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

    }

}
