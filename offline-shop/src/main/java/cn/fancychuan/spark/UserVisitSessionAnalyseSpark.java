package cn.fancychuan.spark;

import cn.fancychuan.conf.ConfigurationManager;
import cn.fancychuan.constant.Constants;
import cn.fancychuan.dao.ITaskDAO;
import cn.fancychuan.dao.impl.DAOFactory;
import cn.fancychuan.domain.Task;
import cn.fancychuan.mock.MockData;
import cn.fancychuan.util.DateUtils;
import cn.fancychuan.util.ParamUtils;
import cn.fancychuan.util.StringUtils;
import cn.fancychuan.util.ValidUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.Accumulator;
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

import java.util.Date;
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
        // 使用自定义累加器
        Accumulator<String> accumulator = sc.accumulator("", new SessionArrgStatAccumulator());
        // 过滤掉非目标数据
        JavaPairRDD<String, String> filtedSession = filterSession(sessionid2AggrInfoRDD, taskParam, accumulator);
        System.out.println("过滤前的条数：" + sessionid2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple2 : sessionid2AggrInfoRDD.take(10)) {
            System.out.println(tuple2._1 + " : " + tuple2._2);
        }
        System.out.println("过滤后的条数：" + filtedSession.count());
        System.out.println("自定义累加器：" + accumulator.value());

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
                    Date startTime = null;
                    Date endTime = null;
                    int stepLength = 0;
                    // 遍历session所有的访问行为
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        // 提取搜索关键词、点击品类字段
                        String searchKeyword = row.getString(5);
                        Long clickCategoryId = (Long) row.get(6);
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
                        // 取出行为时间，得到访问时间访问
                        Date actionTime = DateUtils.parseTime(row.getString(4));
                        if (startTime == null) {
                            startTime = actionTime;
                        }
                        if (endTime == null) endTime = actionTime;
                        if (actionTime.before(startTime)) startTime = actionTime;
                        if (actionTime.after(endTime)) endTime = actionTime;
                        // 访问步长递增
                        stepLength ++;
                    }
                    String keywords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                    Long visitLength = endTime.getTime() - startTime.getTime(); // 访问时长
                    String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                            + Constants.FIELD_SEARCH_KEYWORDS + "=" + keywords + "|"
                            + Constants.FIELD_CATEGORY_ID + "=" + clickCategoryIds + "|"
                            + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                            + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
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

    /**
     * 按照指定条件过滤session
     * 匿名内部类（算子函数）访问外部的对象，是要给外部的对象使用final修饰的 TODO：为什么？
     */
    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessRDD
            , final JSONObject taskParam
            , Accumulator<String> accumulator) {
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        JavaPairRDD<String, String> filtedRDD = sessRDD.filter(item -> {
            String aggrInfo = item._2;
            // 按照年龄过滤
            if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                return false;
            }
            // 按照职业过滤
            if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                return false;
            }
            // 按照城市过滤
            if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                return false;
            }
            // 按照性别进行过滤 男/女  男，女
            if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                return false;
            }
            // 按照搜索词进行过滤 session搜索的关键词只要有一个跟过滤条件中的关键词一样就可以
            if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                return false;
            }
            // 按照点击品类id进行过滤
            if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                return false;
            }

            // 能走到这里说明是需要的session，这个时候对session进行统计
            accumulator.add(Constants.SESSION_COUNT);
            long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
            long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
            // 计算访问时长范围
            if (visitLength >= 1 && visitLength <= 3) accumulator.add(Constants.TIME_PERIOD_1s_3s);
            else if (visitLength >= 4 && visitLength <= 6) accumulator.add(Constants.TIME_PERIOD_4s_6s);
            else if (visitLength >= 7 && visitLength <= 9) accumulator.add(Constants.TIME_PERIOD_7s_9s);
            else if (visitLength >= 10 && visitLength <= 30) accumulator.add(Constants.TIME_PERIOD_10s_30s);
            else if (visitLength >= 30 && visitLength <= 60) accumulator.add(Constants.TIME_PERIOD_30s_60s);
            else if(visitLength > 60 && visitLength <= 180) accumulator.add(Constants.TIME_PERIOD_1m_3m);
            else if(visitLength > 180 && visitLength <= 600) accumulator.add(Constants.TIME_PERIOD_3m_10m);
            else if(visitLength > 600 && visitLength <= 1800) accumulator.add(Constants.TIME_PERIOD_10m_30m);
            else if(visitLength > 1800) accumulator.add(Constants.TIME_PERIOD_30m);
            // 计算访问步长
            if(stepLength >= 1 && stepLength <= 3) accumulator.add(Constants.STEP_PERIOD_1_3);
            else if(stepLength >= 4 && stepLength <= 6) accumulator.add(Constants.STEP_PERIOD_4_6);
            else if(stepLength >= 7 && stepLength <= 9) accumulator.add(Constants.STEP_PERIOD_7_9);
            else if(stepLength >= 10 && stepLength <= 30) accumulator.add(Constants.STEP_PERIOD_10_30);
            else if(stepLength > 30 && stepLength <= 60) accumulator.add(Constants.STEP_PERIOD_30_60);
            else if(stepLength > 60) accumulator.add(Constants.STEP_PERIOD_60);

            return true;
        });
        return filtedRDD;
    }
}
