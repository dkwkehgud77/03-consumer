package com.exam.worker;

import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class PipelineSetting {
    public List<Schema> createSchema(JSONArray beforeJsonArray) throws IOException {
        JSONArray avroJsonArray = new JSONArray();
        List<Schema> avroSchemaList = new ArrayList<>();

        try {
            // 기존 Json 객체에서 Avro 포맷의 Json 객체로 변환
            for(Object obj : beforeJsonArray){
                JSONObject beforeSchemaJson = (JSONObject) obj;
                JSONObject avroSchemaJson = new JSONObject();

                avroSchemaJson.put("type", "record");
                String name = String.valueOf(beforeSchemaJson.get("name"));
                avroSchemaJson.put("name", name);
                avroSchemaJson.put("namespace", "com.exam");

                JSONObject fieldsObject = (JSONObject)beforeSchemaJson.get("fields");
                JSONArray fieldsArray = new JSONArray();
                for (Object entryObj : fieldsObject.entrySet()) {
                    Map.Entry<String, String> entry = (Map.Entry<String, String>) entryObj;
                    JSONObject field = new JSONObject();
                    field.put("name", entry.getKey());
                    field.put("type", entry.getValue().replace("integer","int"));
                    fieldsArray.add(field);
                }

                avroSchemaJson.put("fields", fieldsArray);

                // Avro 스키마로 생성되는지 확인
                Schema avroSchema = new Schema.Parser().parse(avroSchemaJson.toJSONString());
                avroSchemaList.add(avroSchema);
                System.out.println("Avro schema created succeessfully ... " + name);
//                System.out.println("Avro schema for " + name + ":\n" + avroSchema.toString(true) + "\n");
                avroJsonArray.add(avroSchemaJson);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Avro 포맷의 Json 파일로 덤프
        String jsonFilePath = "./src/main/resources/schema/schema_avro.json";
        try(FileWriter avroJsonFile = new FileWriter(jsonFilePath)){
            avroJsonFile.write(avroJsonArray.toJSONString());
            avroJsonFile.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Avro schema Json dumped succeessfully ...");
        return avroSchemaList;
    }

    public void createTopic(List<Schema> avroSchemaList) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093");

        final int numPartitions = 3;  // 토픽 파티션 수
        final int replicationFactor = 3;  // 리플리카 팩터 수


        AdminClient admin = AdminClient.create(props);
        try {
            for(Schema avroSchema : avroSchemaList) {
                String topicName = avroSchema.getName();

                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
                admin.createTopics(Collections.singletonList(newTopic));

                System.out.println("Topic created successfully... " + topicName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            admin.close();
        }

    }


    public void createTable(List<Schema> avroSchemaList) {
        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bank", "consumer", "rlaehgud1!");
            stmt = conn.createStatement();

            for(Schema avroSchema : avroSchemaList) {

                String tableName = avroSchema.getName();
                List<Schema.Field> fields = avroSchema.getFields();

                StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
                for (Schema.Field field : fields) {
                    String name = field.name();
                    Schema.Type type = field.schema().getType();
                    String sqlType = "";
                    switch (type) {
                        case BOOLEAN:
                            sqlType = "BOOLEAN";
                            break;
                        case INT:
                            sqlType = "INT";
                            break;
                        case LONG:
                            sqlType = "BIGINT";
                            break;
                        case FLOAT:
                            sqlType = "FLOAT";
                            break;
                        case DOUBLE:
                            sqlType = "DOUBLE";
                            break;
                        case STRING:
                            sqlType = "VARCHAR(255)";
                            break;
                        default:
                            // TODO: handle other data types if necessary
                            break;
                    }
                    sql.append(name).append(" ").append(sqlType).append(",");
                }
                sql.deleteCharAt(sql.length() - 1);
                sql.append(");");
//                System.out.println(sql.toString());
                stmt.executeUpdate(sql.toString());
                System.out.println("Table created successfully... " + tableName);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
