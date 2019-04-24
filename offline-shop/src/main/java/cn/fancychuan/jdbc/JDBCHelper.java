package cn.fancychuan.jdbc;

import cn.fancychuan.conf.ConfigurationManager;
import cn.fancychuan.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class JDBCHelper {

    // 1. 加载驱动
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 2. 实现单例化
    private static JDBCHelper instance = null;

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }


    private LinkedList<Connection> datasource = new LinkedList<>();

    /**
     * 私有的构造方法，用来初始化数据库连接池
     */
    private JDBCHelper() {
        Integer datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        for (int i=0; i<datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection connection = DriverManager.getConnection(url, user, password);
                datasource.push(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 线程安全的获取连接的方法
     */
    public synchronized Connection getConnection() {
        if (datasource.size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删改sql语句的方法
     */
    public int executeUpdate(String sql, Object[] params) {
        Connection conn = null;
        PreparedStatement pstmt;
        int rtn = 0;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rtn = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 执行查询sql语句
     */
    public void executeQuery(String sql, Object[] params, QueryClassback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL
     *
     * 是JDBC的一个高级功能，批量执行只需要编译一次，可以大大提升性能
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            // 取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            if (paramsList != null && paramsList.size() > 0) {
                for (Object[] parsms : paramsList) {
                    for (int i = 0; i < parsms.length; i++) {
                        pstmt.setObject(i + 1, parsms[i]);
                    }
                    pstmt.addBatch();
                }
                // 执行批量sql
                rtn = pstmt.executeBatch();
                // 手动提交
                conn.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rtn;
    }


    /**
     * 内部类： 查询回调接口
     */
    public interface QueryClassback {
        // 处理查询结果
        void process(ResultSet resultSet) throws Exception;
    }
}
