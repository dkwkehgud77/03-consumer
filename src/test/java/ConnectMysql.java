import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectMysql {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/kakao"; // 데이터베이스 URL
        String username = "consumer"; // MySQL 사용자 이름
        String password = "rlaehgud1!"; // MySQL 암호

        // MySQL 데이터베이스 연결 설정
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {

            // 새 레코드 삽입을 위한 SQL 쿼리문 작성
            String sql = "INSERT INTO messages (key, value) VALUES (1, 'test')";

            // SQL 쿼리문 실행
            int rowsAffected = stmt.executeUpdate(sql);

            // 결과 출력
            System.out.println(rowsAffected + " row(s) affected");

        } catch (SQLException ex) {
            System.err.println("SQL exception occurred: " + ex.getMessage());
        }
    }
}
