package cn.fancychuan;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JDBCCRUDTest {

    public static void main(String[] args) {
        insert();
    }

    public static void insert() {
        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:msyql://s03:3306/shoplog", "root", "123456");
            stmt = conn.createStatement();
            String sql = "";
            int num = stmt.executeUpdate(sql);
            System.out.println("影响的行数：" + sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }

        }
    }
}
