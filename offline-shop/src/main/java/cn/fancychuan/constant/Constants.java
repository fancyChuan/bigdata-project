package cn.fancychuan.constant;

/**
 * 常量接口
 */
public interface Constants {

    // 项目配置相关的常量
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String SPARK_LOCAL = "spark.local";

    // Spark作业相关的常量
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyseSpark";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";

    // 任务相关的常量
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
}
