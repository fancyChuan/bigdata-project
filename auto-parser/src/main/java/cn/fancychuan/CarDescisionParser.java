package cn.fancychuan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CarDescisionParser {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("carParser");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        JavaRDD<String> carLinesRDD = sc.textFile("E:\\JavaWorkshop\\bigdata-project\\auto-parser\\data\\car_sample.log", 2);
        JavaRDD<Row> rowJavaRDD = carLinesRDD.flatMap(line -> {
            String createTime = line.substring(0, 19);
            String inputOutput = line.substring(line.indexOf("{"), line.length());
            JSONObject jsonObject = JSON.parseObject(inputOutput);
            String applyCode = jsonObject.getString("applyCode");
            String inputSystem = jsonObject.getString("inputSystem");
            String inputThird = jsonObject.getString("inputThird");
            JSONObject inputSystemJson = JSON.parseObject(inputSystem);
            JSONObject inputThirdJson = JSON.parseObject(inputThird);
            inputSystemJson.putAll(inputThirdJson);
            //
            ArrayList<Row> list = new ArrayList<>();
            for (Map.Entry<String, Object> entry : inputSystemJson.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                Row row = RowFactory.create(applyCode, createTime, key, value);
                list.add(row);
            }

            return list.iterator();
        });

        // 创建DF
        Dataset<Row> df = spark.createDataFrame(rowJavaRDD, DFSchemaInfo.carInputSchema);
        df.show();

        df.groupBy("apply", "create_time");

    }



}
