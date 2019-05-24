package cn.fancychuan;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public interface DFSchemaInfo {

    StructType carInputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("apply_code", DataTypes.StringType, false),
            DataTypes.createStructField("create_time", DataTypes.StringType, true),
            DataTypes.createStructField("var_type", DataTypes.StringType, true),
            DataTypes.createStructField("key", DataTypes.StringType, true),
            DataTypes.createStructField("value", DataTypes.StringType, true)
    ));
}
