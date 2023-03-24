package com.exam.worker;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

class MySQLProcessData {

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private static final Logger logger = LoggerFactory.getLogger(MySQLProcessData.class);

    public MySQLConnectionPool createConnPool()  {
        // MySQLConnectionPool 객체 생성
        MySQLConnectionPool dbPool = new MySQLConnectionPool(
                    RESOURCE_BUNDLE.getString("mysql.url"),
                    RESOURCE_BUNDLE.getString("mysql.username"),
                    RESOURCE_BUNDLE.getString("mysql.password"),
                    Integer.parseInt(RESOURCE_BUNDLE.getString("mysql.maxPoolSize")));
        return dbPool;
    }

    public Connection getConnMySQL(MySQLConnectionPool dbPool) throws SQLException {
        try {
            if(dbPool == null)
                dbPool = createConnPool();
            Connection conn = dbPool.getConnection();
            return conn;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> readOffsetsFromDB(Connection conn, String topicName, int partitionId){
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // DB에서 저장된 offset 정보를 읽어온다.
        try {
            String query = String.format("SELECT * FROM kafka_offsets where topic='%s' and `partition`=%s",topicName, partitionId);
            System.out.println(query);
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String topic = rs.getString("topic");
                int partition = rs.getInt("partition");
                long offset = rs.getLong("offset");
                offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return offsets;
    }

    public void saveOffsetToDB(Connection conn, Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {

            String sql = "INSERT INTO kafka_offsets (offset, topic, `partition`)\n" +
                    "VALUES (?, ?, ?)\n" +
                    "ON DUPLICATE KEY UPDATE\n" +
                    "    offset = ?,\n" +
                    "    topic = ?,\n" +
                    "    `partition` = ?";
//            String sql = "UPDATE kafka_offsets SET offset = ? WHERE topic = ? AND `partition` = ?";
            PreparedStatement stmt = conn.prepareStatement(sql);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long offset = entry.getValue().offset();
                stmt.setLong(1, offset);
                stmt.setString(2, tp.topic());
                stmt.setInt(3, tp.partition());
                stmt.setLong(4, offset);
                stmt.setString(5, tp.topic());
                stmt.setInt(6, tp.partition());
                stmt.executeUpdate();

                logger.info(String.format("offsets updated successfuly... topic:%s partition:%d",tp.topic(), tp.partition()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public PreparedStatement setParameter(PreparedStatement stmt, int parameterIndex, Object value, String sqlType) throws SQLException {
        switch (sqlType) {
            case "STRING":
                stmt.setString(parameterIndex, value.toString());
                return stmt;

            case "INT":
                stmt.setInt(parameterIndex, (Integer) value);
                return stmt;

            case "LONG":
                stmt.setLong(parameterIndex, (Long) value);
                return stmt;

            case "DOUBLE":
                stmt.setDouble(parameterIndex, (Double) value);
                return stmt;

            default:
                throw new SQLException("Unknown SQL type: " + sqlType);
        }

    }


}