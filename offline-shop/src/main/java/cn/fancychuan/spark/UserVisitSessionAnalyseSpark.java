package cn.fancychuan.spark;

import cn.fancychuan.conf.ConfigurationManager;
import cn.fancychuan.constant.Constants;
import cn.fancychuan.dao.ITaskDAO;
import cn.fancychuan.dao.impl.DAOFactory;
import cn.fancychuan.domain.Task;
import cn.fancychuan.mock.MockData;
import cn.fancychuan.util.ParamUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

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

    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date >= '" + startDate
                + "' and date <= '" + endDate + "'";
        Dataset actionDF =  sqlContext.sql(sql);
        return actionDF.toJavaRDD();
    }

    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD) {
        actionRDD.mapToPair()
    }
}
