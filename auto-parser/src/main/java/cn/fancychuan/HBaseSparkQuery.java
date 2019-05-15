package cn.fancychuan;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class HBaseSparkQuery implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("readHbase");
        JavaSparkContext sc = new JavaSparkContext(conf);

        new HBaseConfiguration();
    }

}
