package cn.fancychuan.qzpoint.util;

public class DataSourceUtil {
    public static DataSource dataSource = null;

    static {
        try {
            Properties props = new Properties();
            props.setProperty("url", ConfigurationManager.getProperty("jdbc.url"));
            props.setProperty("username", ConfigurationManager.getProperty("jdbc.user"));
            props.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
            props.setProperty("initialSize", "5"); //初始化大小
            props.setProperty("maxActive", "10"); //最大连接
            props.setProperty("minIdle", "5");  //最小连接
            props.setProperty("maxWait", "60000"); //等待时长
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            props.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            props.setProperty("validationQuery", "select 1");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("testOnBorrow", "false");
            props.setProperty("testOnReturn", "false");
            props.setProperty("keepAlive", "true");
            props.setProperty("phyMaxUseCount", "100000");
//            props.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //提供获取连接的方法
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 提供关闭资源的方法【connection是归还到连接池】
    // 提供关闭资源的方法 【方法重载】3 dql
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
