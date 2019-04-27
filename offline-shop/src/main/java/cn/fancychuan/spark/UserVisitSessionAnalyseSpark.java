package cn.fancychuan.spark;

import cn.fancychuan.conf.ConfigurationManager;
import cn.fancychuan.constant.Constants;
import cn.fancychuan.dao.ITaskDAO;
import cn.fancychuan.dao.impl.DAOFactory;
import cn.fancychuan.domain.Task;
import cn.fancychuan.mock.MockData;
import cn.fancychuan.util.ParamUtils;
import cn.fancychuan.util.StringUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析spark作业
 *
 *
 */
public class UserVisitSessionAnalyseSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf); // TODO:为什么要用这个而不是SparkContext
        SQLContext sqlContext = getSQLContext(sc.sc());
        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 获取用户所创建任务的参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 获取指定日期范围内的RDD
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        // 对行为数据按照session粒度聚合，同时获取到用户信息
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        // JavaSparkContext需要关闭
        sc.close();
    }

    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
           return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据，只有本地模式才会生成
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期的actionRDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = null;
        try {
            sql = "select * from user_visit_action where date >= '" + startDate
                    + "' and date <= '" + endDate + "'";
        } catch (Exception e) {
            e.printStackTrace();
        }
        Dataset actionDF =  sqlContext.sql(sql);
        return actionDF.toJavaRDD();
    }

    /**
     * 对行为数据按照session粒度进行聚合，同时获取到用户的信息
     * 结果：
     *      <sessionId, actionData+userInfo>
     * 所以需要根据userid对两个RDD进行json，最后再拼接
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {
        // 1. 先提取到sessionId作为RDD的key
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 3258845628763444421L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        // 2. 按照sessionId汇总
        JavaPairRDD<String, Iterable<Row>> sessid2rdds = sessionid2ActionRDD.groupByKey();
        // 3. 这里对行为数据聚合拼接，结果为 <userId, actionData>
        JavaPairRDD<Long, String> userId2AggrPartInfoRDD = sessid2rdds.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                private static final long serialVersionUID = -2535958373572073846L;

                @Override
                public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                    String sessionid = stringIterableTuple2._1;
                    Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                    StringBuffer searchKeyWordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                    Long userId = null;
                    // 遍历session所有的访问行为
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        // 提取搜索关键词、点击品类字段
                        String searchKeyword = row.getString(5);
                        Long clickCategoryId = row.getLong(6);
                        if (userId == null) {
                            userId = row.getLong(1);
                        }
                        // 需要处理null的情况
                        if (StringUtils.isNotEmpty(searchKeyword)) {
                            if (!searchKeyWordsBuffer.toString().contains(searchKeyword)) {
                                searchKeyWordsBuffer.append(searchKeyword + ",");
                            }
                        }
                        if (clickCategoryId != null) {
                            if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                clickCategoryIdsBuffer.append(clickCategoryId + ",");
                            }
                        }
                    }
                    String keywords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                    String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                            + Constants.FIELD_SEARCH_KEYWORDS + "=" + keywords + "|"
                            + Constants.FIELD_CATEGORY_ID + "=" + clickCategoryIds + "|";
                    return new Tuple2<>(userId, partAggrInfo);
                }
        });

        // 4. 查询所有用户数据，结果为 <userId, userInfo>
        JavaRDD<Row> userInfoRDD = sqlContext.sql("select * from user_info").toJavaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDd = userInfoRDD.mapToPair(row -> new Tuple2<Long, Row>(row.getLong(0), row));
        // 5. 进行join操作并拼接成想要的数据格式
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userId2AggrPartInfoRDD.join(userid2InfoRDd);
        JavaPairRDD<String, String> wantRDD = userid2FullInfoRDD.mapToPair(item -> { // item这里是整个userid2FullInfoRDD的一个元素，为<userId, <sessInfo, userInfo>>
            Long userId = item._1;
            String sessInfo = item._2._1;
            Row userInfoRow = item._2._2;

            // 需要把sessionID提取出来
            String sessionId = StringUtils.getFieldFromConcatString(sessInfo, "\\|", Constants.FIELD_SESSION_ID);
            // 取出用户信息
            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);

            String wantInfo = sessInfo + "|"
                    + Constants.FIELD_AGE + "=" + age + "|"
                    + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constants.FIELD_CITY + "=" + city + "|"
                    + Constants.FIELD_SEX + "=" + sex;
            return new Tuple2<>(sessionId, wantInfo);
        });
        return wantRDD;
    }
}
