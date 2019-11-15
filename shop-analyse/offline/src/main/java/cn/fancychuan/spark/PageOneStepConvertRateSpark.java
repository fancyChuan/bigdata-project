package cn.fancychuan.spark;

import cn.fancychuan.constant.Constants;
import cn.fancychuan.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * 页面单跳转化率spark作业
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {
        // 1.构造spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
        // 2.生成模拟数据
        SparkUtils.mockData(sc, sqlContext);
        // 3.查询任务，获取任务的参数

    }
}
