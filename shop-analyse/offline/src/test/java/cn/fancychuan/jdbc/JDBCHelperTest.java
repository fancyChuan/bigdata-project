package cn.fancychuan.jdbc;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * JDBC辅助组建单元测试
 */
public class JDBCHelperTest {

    public static void main(String[] args) {
        JDBCHelper instance = JDBCHelper.getInstance();
        // 测试普通的增删改语句
        System.out.println(instance.executeUpdate(
                "insert into test_user(name, age) values (?, ?)",
                new Object[]{"张三", 23}));


        // 测试查询语句
        final Map<String, Object> testUser = new HashMap<>();
        instance.executeQuery(
                "select * from test_user where id=?",
                new Object[] {1},
                new JDBCHelper.QueryClassback() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            String name = rs.getString(2);
                            int age = rs.getInt(3);
                            testUser.put("name", name);
                            testUser.put("age", age);
                        }
                    }
                }
        );
        System.out.println(testUser);

        // 测试批量执行语句
        String sql = "insert into test_user(name, age) values (?, ?)";
        List<Object[]> paramsList = new LinkedList<>();
        paramsList.add(new Object[] {"fancy", 22});
        paramsList.add(new Object[] {"fancyChuan", 33});
        System.out.println(instance.executeBatch(sql, paramsList));
    }
}
