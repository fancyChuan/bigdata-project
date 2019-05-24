package cn.fancychuan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

            // 处理入参
            JSONObject inputSystemJson = JSON.parseObject(inputSystem);
            JSONObject inputThirdJson = JSON.parseObject(inputThird);
            inputSystemJson.putAll(inputThirdJson);
            HashMap<String, TreeMap<Integer, String>> mergedInput = getMergedInput(inputSystemJson);
            // 处理出参
            String output = jsonObject.getString("resultDecision");
            HashMap<String, TreeMap<Integer, String>> mergedOutput = getMergedOutput(output);
            // 对数组变量的多个元素合并
            ArrayList<Row> list = new ArrayList<>();
            for (Map.Entry<String, TreeMap<Integer, String>> entry : mergedInput.entrySet()) {
                String key = entry.getKey();
                String value = StringUtils.join(entry.getValue().values(), "|");
                Row row = RowFactory.create(applyCode, createTime, "input", key, value);
                list.add(row);
            }
            for (Map.Entry<String, TreeMap<Integer, String>> entry : mergedOutput.entrySet()) {
                String key = entry.getKey();
                String value = StringUtils.join(entry.getValue().values(), "|");
                Row row = RowFactory.create(applyCode, createTime, "output", key, value);
                list.add(row);
            }
            return list.iterator();
        });

        // 创建DF
        Dataset<Row> df = spark.createDataFrame(rowJavaRDD, DFSchemaInfo.carInputSchema);
        df.createOrReplaceTempView("car_parser_result");
        spark.sql("select * from car_parser_result where var_type = 'output'").show(false);

    }

    /**
     * 处理入参的数组型变量
     */
    private static HashMap<String, TreeMap<Integer, String>> getMergedInput(JSONObject parsedJson) {
        HashMap<String, TreeMap<Integer, String>> varTreeMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            String key = entry.getKey();
            Integer index = 1;
            if (key.contains("[")) {
                index = Integer.valueOf(key.substring(key.indexOf("[") + 1, key.indexOf("]")));
                key = key.substring(0, key.indexOf("["));
            }
            String value = entry.getValue().toString();
            TreeMap<Integer, String> indexValueMap = varTreeMap.get(key);
            if (indexValueMap == null) {
                indexValueMap = new TreeMap<>();
            }
            indexValueMap.put(index, value);
            varTreeMap.put(key, indexValueMap);
        }
        return varTreeMap;
    }

    /**
     * 处理出参型的数组变量
     */
    private static HashMap<String, TreeMap<Integer, String>> getMergedOutput(String output) {
        HashMap<String, TreeMap<Integer, String>> varTreeMap = new HashMap<>();
        for (String s : output.split(",")) {
            Integer index = 1;
            String[] split = s.replace(" ", "").split("=");
            String key = split[0];
            if (key.contains("[")) {
                index = Integer.valueOf(key.substring(key.indexOf("[") + 1, key.indexOf("]")));
                key = key.substring(0, key.indexOf("["));
            }
            String value = "";
            if (split.length == 2) {
                value = split[1];
            }
            // 把变量写入中间Map
            TreeMap<Integer, String> indexValueMap = varTreeMap.get(key);
            if (indexValueMap == null) {
                indexValueMap = new TreeMap<>();
            }
            indexValueMap.put(index, value);
            varTreeMap.put(key, indexValueMap);
        }
        return varTreeMap;
    }

}
