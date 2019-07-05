package cn.fancychuan.spark;

import cn.fancychuan.conf.ConfigurationManager;
import cn.fancychuan.constant.Constants;
import cn.fancychuan.dao.*;
import cn.fancychuan.dao.impl.DAOFactory;
import cn.fancychuan.domain.*;
import cn.fancychuan.mock.MockData;
import cn.fancychuan.util.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;


import java.util.*;

/**
 * 用户访问session分析spark作业
 *
 *
 */
public class UserVisitSessionAnalyseSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local")
                .set("spark.storage.memoryFraction", "0.6")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{CategorySortKey.class}) // 注册要序列化的类
                ;
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
        // actionRDD是一个公共RDD，在后面需要多次使用，重构后只使用一次
        // 先得到 <sessionid, actionRow>
        // JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2), row));
        // * 使用mapPartitionsToPair优化上一行的代码
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapPartitionsToPair(rows -> {
            ArrayList<Tuple2<String, Row>> list = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                list.add(new Tuple2<>(row.getString(2), row));
            }
            return list.iterator();
        });
        // sessionid2actionRDD后面用到2次，需要做持久化
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        // 对行为数据按照session粒度聚合，同时获取到用户信息
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sessionid2actionRDD, sqlContext);
        // 使用自定义累加器
        Accumulator<String> accumulator = sc.accumulator("", new SessionArrgStatAccumulator());
        // 过滤掉非目标数据
        JavaPairRDD<String, String> filtedSession = filterSessionAndStat(sessionid2AggrInfoRDD, taskParam, accumulator);
        filtedSession = filtedSession.persist(StorageLevel.MEMORY_ONLY()); // 也是用多次，持久化
        System.out.println("过滤前的条数：" + sessionid2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple2 : sessionid2AggrInfoRDD.take(5)) {
            System.out.println(tuple2._1 + " : " + tuple2._2);
        }
        System.out.println("过滤后的条数：" + filtedSession.count());
        System.out.println("自定义累加器：" + accumulator.value());
        // 把统计后的结果写入mysql
        // calculateAndWrite2Mysql(accumulator.value(), taskid);
        System.out.println("写入完成，准备抽取session");

        // 过滤后的数据与行为数据关联，生成明细RDD
        JavaPairRDD<String, Row> sessionid2detailRDD = filtedSession.join(sessionid2actionRDD)
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._1, tuple2._2._2));
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY()); // 使用多次，持久化处理
        // 随机抽取session
        randomExtractSession(sc, filtedSession, taskid, sessionid2detailRDD);
        System.out.println("抽取完成，准备获取top10品类");
        // top10品类
        List<Tuple2<CategorySortKey, String>> top10Category = getTop10Category(taskid, sessionid2detailRDD);
        System.out.println(top10Category);
        // top10热门session
        System.out.println("准备获取热门session");
        getTop10Session(sc, taskid, top10Category, sessionid2detailRDD);
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
        Dataset actionDF =  sqlContext.sql(sql); // 读出数据可能分区数太少
        return actionDF.toJavaRDD().repartition(100); // 进行重分区
    }

    /**
     * 对行为数据按照session粒度进行聚合，同时获取到用户的信息
     * 结果：
     *      <sessionId, actionData+userInfo>
     * 所以需要根据userid对两个RDD进行json，最后再拼接
     */
//    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {
        // 1. 先提取到sessionId作为RDD的key
//        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//            private static final long serialVersionUID = 3258845628763444421L;
//
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                return new Tuple2<>(row.getString(2), row);
//            }
//        });
    private static JavaPairRDD<String, String> aggregateBySession(
            JavaSparkContext sc, JavaPairRDD<String, Row> sessionid2ActionRDD, SQLContext sqlContext) {
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
                            + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                            + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
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
/*      // 上面的userId2AggrPartInfoRDD.join(userid2InfoRDd)可能会发生数据倾斜。这里可以采用map join的方式。如下面注释的代码
        List<Tuple2<Long, Row>> userinfos = userid2InfoRDd.collect();
        final Broadcast<List<Tuple2<Long, Row>>> userInfoBroadcast = sc.broadcast(userinfos);
        JavaPairRDD<String, String> wantRDD = userId2AggrPartInfoRDD.mapToPair(tuple -> {
            List<Tuple2<Long, Row>> userInfos = userInfoBroadcast.value();
            HashMap<Long, Row> userInfoMap = new HashMap<>();
            for (Tuple2<Long, Row> userInfo : userInfos) {
                userInfoMap.put(userInfo._1, userInfo._2);
            }
            String partAggrInfo = tuple._2;
            Row userInfoRow = userInfoMap.get(tuple._1);

            String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);

            String fullAggrInfo = partAggrInfo + "|"
                    + Constants.FIELD_AGE + "=" + age + "|"
                    + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constants.FIELD_CITY + "=" + city + "|"
                    + Constants.FIELD_SEX + "=" + sex;
            return new Tuple2<>(sessionId, fullAggrInfo);
        });
*/
/*      // 还有一种方式，就是把导致数据倾斜的key通过采样找到，然后过滤出来，单独进行join。两次join后再合并
        JavaPairRDD<Long, String> sampleRDD = userId2AggrPartInfoRDD.sample(false, 0.1, 9); // 是否放回采样
        JavaPairRDD<Long, Long> sortedSampleRDD = sampleRDD.mapToPair(tuple -> new Tuple2<>(tuple._1, 1L)).reduceByKey((x1, x2) -> x1 + x2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).sortByKey();
        // 取出数量最多的一个key
        Long skewedUserid = sortedSampleRDD.take(1).get(0)._2;
        JavaPairRDD<Long, String> skewedRDD = userId2AggrPartInfoRDD.filter(tuple -> tuple._1.equals(skewedUserid));
        JavaPairRDD<Long, String> commonRDD = userId2AggrPartInfoRDD.filter(tuple -> !tuple._1.equals(skewedUserid));
        JavaPairRDD<Long, Tuple2<String, Row>> joinRDD1 = skewedRDD.join(userid2InfoRDd);
        JavaPairRDD<Long, Tuple2<String, Row>> joinRDD2 = commonRDD.join(userid2InfoRDd); // 正常的key所对应的RDD
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = joinRDD1.union(joinRDD2);
        // userid2FullInfoRDD.mapToPair(...)
*/
        return wantRDD;
    }

    /**
     * 按照指定条件过滤session
     * 匿名内部类（算子函数）访问外部的对象，是要给外部的对象使用final修饰的 TODO：为什么？
     */
    private static JavaPairRDD<String, String> filterSessionAndStat(JavaPairRDD<String, String> sessRDD
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


    /**
     * 把统计结果写入mysql
     * 【注意】
     *      value是统计后的累积器的值，需要在一个action出发job以后才能有值，这个函数的执行才有意义
     */
    private static void calculateAndWrite2Mysql(String value, long taskid) {
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 随机抽取session：按照每个小时的session占比数来分层抽样
     */
    private static void randomExtractSession(JavaSparkContext sc,
                                             JavaPairRDD<String, String> session2AggrInfoRDD,
                                             final long taskid,
                                             JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, String> time2sessionRDD = session2AggrInfoRDD.mapToPair(tuple -> {
            String sessionid = tuple._1;
            String aggrInfo = tuple._2;
            String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
            String dateHour = DateUtils.getDateHour(startTime);
            return new Tuple2<>(dateHour, aggrInfo);
        });
        Map<String, Long> countMap = time2sessionRDD.countByKey();
        // 把统计结果存为 <date, <hour, count>>
        HashMap<String, Map<String, Long>> dayHourCountMap = new HashMap<>();
        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            String dateHour = entry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            long count = entry.getValue();

            Map<String, Long> hourCountMap = dayHourCountMap.get(date);
            if (hourCountMap == null) { // 如果当天还创建过，需要新建一个
                hourCountMap = new HashMap<>();
            }
            hourCountMap.put(hour, count);  // 把当前遍历到的hour的统计结果放进去
            dayHourCountMap.put(date, hourCountMap); // 把结果放回去Map中替换掉旧Map
        }
        // 每天需要抽取的数量
        long extractNumPerDay = 100 / dayHourCountMap.size();
        // 最终需要的随机数结构 <date, <hour, [1,2,4,6]>
        // 在算子中使用的时候，需要对变量加上final修饰
        final HashMap<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();
        // 开始遍历每一天
        for (Map.Entry<String, Map<String, Long>> entry : dayHourCountMap.entrySet()) {
            Random random = new Random();
            String date = entry.getKey();
            long daySessCnt = 0; // 当天的session总数

            // 获取到每天的小时粒度的计数结果
            Map<String, Long> hourCountMap = entry.getValue();
            // 第一次遍历统计出每天的session数
            for (Long count : hourCountMap.values()) {
                daySessCnt += count;
            }
            // 开始遍历每小时的统计情况，并生成随机数组
            Map<String, List<Integer>> hourRandomListMap = dateHourExtractMap.get(date);
            if (hourRandomListMap == null) {
                hourRandomListMap = new HashMap<>();
            }
            // 第二次遍历，生成随机数
            for (Map.Entry<String, Long> hourCntEntry : hourCountMap.entrySet()) {
                String hour = hourCntEntry.getKey();
                long count = hourCntEntry.getValue();
                long hourExtractNum = (int)((double) count / daySessCnt * extractNumPerDay);
                if (hourExtractNum > count) { // 当一天只有一个小时的时候，hourExtractNum就会等于 extractNumPerDay，可能比较大
                    hourExtractNum = count;
                }
                LinkedList<Integer> randomList = new LinkedList<>();
                for (long i = 0; i < hourExtractNum; i++) {
                    int ranInt = random.nextInt((int) count);
                    while (randomList.contains(ranInt)) {
                        ranInt = random.nextInt((int) count);
                    }
                    randomList.add(ranInt);
                }
                hourRandomListMap.put(hour, randomList); // 其实这个地方是可以放在前面的，因为Map里面存的是对象的索引
                dateHourExtractMap.put(date, hourRandomListMap); // 覆盖掉原来的结果
            }
        }

        // 把大变量做成广播变量
        final Broadcast<HashMap<String, Map<String, List<Integer>>>> dateHourExtractMapBroadcast = sc.broadcast(dateHourExtractMap);

        // 获取需要随机抽取的索引
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionRDD.groupByKey();
        JavaPairRDD<String, String> extractSessionsRDD = time2sessionsRDD.flatMapToPair(tuple2 -> {
            /**
             * 这个flatMapToPair算子使用了外部变量dateHourExtractMap和taskid，dateHourExtractMap比较大，可以将其作为广播变量
             * 使用广播变量的时候，通过broadcast.getValue() 或者  broadcast.value() 获取到变量
             */
            List<Tuple2<String, String>> extractSessions = new ArrayList<>();
            String dateHour = tuple2._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            // 聚合（小时粒度）后的行为数据的集合
            Iterator<String> iterator = tuple2._2.iterator();
            // 先获取广播变量
            final HashMap<String, Map<String, List<Integer>>> dateHourExtractMap2 = dateHourExtractMapBroadcast.getValue();
            // 拿到随机数（随机抽取的种子）
            List<Integer> randomList = dateHourExtractMap2.get(date).get(hour); // 这里使用广播变量dateHourExtractMap2而不是外部变量dateHourExtractMap
            ISessionRandomExtractDAO randomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
            int index = 0;
            // 遍历行为数据集合，并把要抽取的数据保存到mysql
            while (iterator.hasNext()) {
                String aggrInfo = iterator.next();
                if (randomList.contains(index)) { // 序号与随机数相同，则表示是目标数据
                    String sessionid = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskid(taskid);
                    sessionRandomExtract.setSessionid(sessionid);
                    sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME));
                    sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                    sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                    randomExtractDAO.insert(sessionRandomExtract);
                    // 把sessionid 作为后面要有的索引返回
                    extractSessions.add(new Tuple2<>(sessionid, sessionid));
                }
                index ++;
            }
            return extractSessions.iterator();
        });
        // 获取明细数据
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionsRDD.join(sessionid2actionRDD);
        extractSessionDetailRDD.foreach(tuple -> {
            Row row = tuple._2._2;
            SessionDetail sessionDetail = getSessionDetailInstance(row, taskid);
            ISessionDetailDAO detailDAO = DAOFactory.getSessionDetailDAO();
            detailDAO.insert(sessionDetail);
        });
    }

    /**
     * 使用二次排序获取top10 品类： 包括 点击过的+下单过的+支付过的
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            long taskid,
            JavaPairRDD<String, Row> sessionid2detailRDD) {

        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(tuple2 -> {
                    Row row = tuple2._2;

                    List<Tuple2<Long, Long>> list = new ArrayList<>();

                    Long clickCategoryId = (Long) row.get(6);
                    if (clickCategoryId != null) {
                        list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                    }
                    String orderCategoryIds = row.getString(8);
                    if (orderCategoryIds != null) {
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                        }
                    }
                    String payCategoryIds = row.getString(10);
                    if (payCategoryIds != null) {
                        String[] payCates = payCategoryIds.split(",");
                        for (String cateid : payCates) {
                            list.add(new Tuple2<>(Long.valueOf(cateid), Long.valueOf(cateid)));
                        }
                    }
                    return list.iterator();
                });
        categoryidRDD = categoryidRDD.distinct();
        // 计算每个品类的点击、下单、支付次数，为后面的join准备
        // 计算品类的点击次数， <cateId, count>
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = sessionid2detailRDD
                .filter(tuple -> tuple._2.get(6) != null).coalesce(100) // 使用coalesce算子把分区数减少到100，后面还有个参数决定是否shuffle
                .mapToPair(tuple -> new Tuple2<>(tuple._2.getLong(6), 1L))
                // 如果某个品类点击了100万次，其他品类值点击了几万次，那就有可能发生数据倾斜，因此，在reduceByKey的时候可以传入一个值，用于设置shuffle的reduce端并行度
                .reduceByKey((x, y) -> x + y, 100);
        // 品类的下单次数， <cateId, count>
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = sessionid2detailRDD
                .filter(tuple -> tuple._2.get(8) != null)
                .flatMapToPair(tuple -> {
                    List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                    String[] ids = tuple._2.getString(8).split(",");
                    for (String id : ids) {
                        list.add(new Tuple2<>(Long.valueOf(id), 1L));
                    }
                    return list.iterator();
                })
                .reduceByKey((x, y) -> x + y); // 这个地方也可能出现数据倾斜
        /*

                // 下面使用加随机数形成新的key来解决数据倾斜的问题
                // (1.1)先给每个key加上随机数
                .mapToPair(tuple -> {
                    long key = tuple._1;
                    long value = tuple._2;
                    Random random = new Random();
                    int prefix = random.nextInt(10); // 用于改造key的随机数
                    return new Tuple2<String, Long>(prefix + "_" + key, value);
                })
                // (1.2)进行局部聚合统计
                .reduceByKey((t1, t2) -> t1 + t2)
                // (2.)把key还原，进行全局聚合统计
                .mapToPair(pt -> new Tuple2<>(Long.valueOf(pt._1.split("_")[1]), pt._2))
                .reduceByKey((t1, t2) -> t1 + t2);
        */
        // 支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = sessionid2detailRDD
                .filter(tuple -> tuple._2.get(10) != null)
                .flatMapToPair(tuple -> {
                    List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                    String[] ids = tuple._2.getString(10).split(",");
                    for (String id : ids) {
                        list.add(new Tuple2<>(Long.valueOf(id), 1L));
                    }
                    return list.iterator();
                })
                .reduceByKey((x, y) -> x + y);
        // 把品类的统计指标组合
        JavaPairRDD<Long, String> categoryid2countRDD = categoryidRDD
                // 跟点击计数合并
                .leftOuterJoin(clickCategoryId2CountRDD).mapToPair(tuple -> {
                    Long sessiongId = tuple._1;
                    Long clickCount = 0L;
                    Optional<Long> optional = tuple._2._2;
                    if (optional.isPresent()) {
                        clickCount = optional.get();
                    }
                    String value = Constants.FIELD_CATEGORY_ID + "=" + sessiongId + "|"
                            + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                    return new Tuple2<>(sessiongId, value);
                // 跟下单计数合并
                }).leftOuterJoin(orderCategoryId2CountRDD).mapToPair(tuple -> {
                    Long sessionId = tuple._1;
                    String value = tuple._2._1;
                    Optional<Long> optional = tuple._2._2;
                    Long orderClick = 0L;
                    if (optional.isPresent()) {
                        orderClick = optional.get();
                    }
                    value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderClick;
                    return new Tuple2<>(sessionId, value);
                // 跟支付计数合并
                }).leftOuterJoin(payCategoryId2CountRDD).mapToPair(tuple -> {
                    Long sessionId = tuple._1;
                    String value = tuple._2._1;
                    Optional<Long> optional = tuple._2._2;
                    Long payClick = 0L;
                    if (optional.isPresent()) {
                        payClick = optional.get();
                    }
                    value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payClick;
                    return new Tuple2<>(sessionId, value);
                });
        // 使用自定义二次排序
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(tuple -> {
            Long sessionId = tuple._1;
            String countInfo = tuple._2;
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            return new Tuple2<>(new CategorySortKey(clickCount, orderCount, payCount), countInfo);
        });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        // 获取前10的品类，并写入mysql
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        top10CategoryList.forEach(item -> {
            String countInfo = item._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryId);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);
            top10CategoryDAO.insert(category);
        });

        return top10CategoryList;
    }

    /**
     * 获取top10品类对应的热门session：1.把session对品类的点击情况存入 2.把热门session明细存到mysql
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            long taskid,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        // 第1步： top10品类ID转为一个RDD
        ArrayList<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId, categoryId));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
        // 第2步：1. 计算各个session下点击品类的情况，结果应为： [<cateidA, sessionid1=count1> <cateidA, sessionid2=count2>, ...]
        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailRDD.groupByKey()
                .flatMapToPair(tuple -> {
                    String sessionId = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();
                    // 遍历统计每个session下品类的点击情况
                    HashMap<Long, Long> categoryCountMap = new HashMap<>();
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.get(6) != null) {
                            long categoryId = row.getLong(6);
                            Long count = categoryCountMap.get(categoryId);
                            if (count == null) {
                                count = 0L;
                            }
                            count += 1;
                            categoryCountMap.put(categoryId, count);
                        }
                    }
                    // 遍历统计出来的点击情况，按照品类-session点击数的形式返回，也就是：[<cateidA, sessionid1=count1> <cateidA, sessionid2=count2>, ...]
                    ArrayList<Tuple2<Long, String>> cateSessCountList = new ArrayList<>();
                    for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                        Long categoryId = entry.getKey();
                        Long count = entry.getValue();
                        String value = sessionId + "," + count;
                        cateSessCountList.add(new Tuple2<>(categoryId, value));
                    }
                    return cateSessCountList.iterator();
                    // 这里还有另外一种方法：把最终结果表示为 <cateidA, sessionid1=count1|sessionid2=count2>，这样后面就不需要groupByKey()了
                });
        // 2. 跟top10品类关联，过滤出热门session，结果： [<cateidA, sessionid1=count1> <cateidA, sessionid2=count2>, ...]
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD)
                .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2));
        // 第3步：分组取top10活跃用户，也就是session的点击是最多的那10个，并按照 [(cateA, top1, count),(cateA, top2, count)]存到mysql
        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountRDD.groupByKey() // 这里GroupBy是因为flatMapToPair(func) 的结果是可能包含重复key的，虽然单个func下返回的list不会有重复的key
                .flatMapToPair(tuple -> { // 这里使用flatMapToPair是为了后面把热门session对应的明细存入mysql
                    Long categoryId = tuple._1;
                    Iterator<String> iterator = tuple._2.iterator();
                    // 使用冒泡排序找出top10Session
                    String[] top10Sessions = new String[10];
                    while (iterator.hasNext()) {
                        String sessionCount = iterator.next();
                        long count = Long.valueOf(sessionCount.split(",")[1]);
                        for (int i = 0; i < top10Sessions.length; i++) {
                            if (top10Sessions[i] == null) { // 前10个元素把数组填充满
                                top10Sessions[i] = sessionCount;
                                break;
                            } else {
                                // 冒泡排序法，边前10个边填充边排序
                                long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                                if (count >= _count) { // 大的就插入，并把数组后面的元素全部往后挪一位
                                    for (int j = 9; j > i; j--) {
                                        top10Sessions[j] = top10Sessions[j - 1];
                                    }
                                    top10Sessions[i] = sessionCount;
                                    break;
                                }
                            }
                            // 比数组中元素都要小的，则忽略
                        }
                    }
                    // 把找出来的热门session写入mysql，并返回sessionID，以便后续保存session的明细
                    ArrayList<Tuple2<String, String>> top10SessionList = new ArrayList<>();

                    for (String sessionCount : top10Sessions) {
                        if (sessionCount != null) { // 有可能并没有top10
                            String sessionId = sessionCount.split(",")[0];
                            Long count = Long.valueOf(sessionCount.split(",")[1]);
                            // 封装到domain
                            Top10Session top10Session = new Top10Session();
                            top10Session.setTaskid(taskid);
                            top10Session.setCategoryid(categoryId);
                            top10Session.setSessionid(sessionId);
                            top10Session.setClickCount(count);

                            ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                            top10SessionDAO.insert(top10Session);
                            // 存完mysql之后把session组装成Tuple2返回
                            top10SessionList.add(new Tuple2<>(sessionId, sessionId));
                        }
                    }
                    // 把找到的session返回
                    return top10SessionList.iterator();
                });
        // 第4步：把找到的热门session明细村到mysql，便于J2EE平台查询
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD);
        // 4.1 使用foreach把数据写入mysql
//        sessionDetailRDD.foreach(tuple -> {
//            Row row = tuple._2._2;
//            SessionDetail sessionDetail = getSessionDetailInstance(row, taskid);
//            // 准备写入mysql
//            ISessionDetailDAO detailDAO = DAOFactory.getSessionDetailDAO();
//            detailDAO.insert(sessionDetail);
//        });
        // 4.2 使用foreachPartition批量写入mysql，提高性能
        sessionDetailRDD.foreachPartition(tuples -> {
            ArrayList<SessionDetail> detailArrayList = new ArrayList<SessionDetail>();
            while (tuples.hasNext()) {
                Tuple2<String, Tuple2<String, Row>> tuple = tuples.next();
                Row row = tuple._2._2;
                SessionDetail sessionDetail = getSessionDetailInstance(row, taskid);
                detailArrayList.add(sessionDetail);
            }
            // 批量写入mysql
            ISessionDetailDAO detailDAO = DAOFactory.getSessionDetailDAO();
            detailDAO.insertBatch(detailArrayList);
        });

    }

    /**
     * 把Row对象封装到sessionDetail
     */
    private static SessionDetail getSessionDetailInstance(Row row, long taskid) {
        SessionDetail sessionDetail = new SessionDetail();
        sessionDetail.setTaskid(taskid);
        sessionDetail.setUserid(row.getLong(1));
        sessionDetail.setSessionid(row.getString(2));
        sessionDetail.setPageid(row.getLong(3));
        sessionDetail.setActionTime(row.getString(4));
        sessionDetail.setSearchKeyword(row.getString(5));
        if (row.get(6) == null) sessionDetail.setClickCategoryId(null);
        else sessionDetail.setClickCategoryId(row.getLong(6)); // TODO:为什么这里的long直接获取总是报空指针异常？
        if (row.get(7) == null) sessionDetail.setClickProductId(null);
        else sessionDetail.setClickProductId(row.getLong(7));
        sessionDetail.setOrderCategoryIds(row.getString(8));
        sessionDetail.setOrderProductIds(row.getString(9));
        sessionDetail.setPayCategoryIds(row.getString(10));
        sessionDetail.setPayProductIds(row.getString(11));

        return sessionDetail;
    }
}
