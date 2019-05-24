package cn.fancychuan.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 自定义聚合函数：合并数组型的变量
 */
public class MergeArrayVariablesAggrUDF extends UserDefinedAggregateFunction {

    private StructType inputSchema;
    private StructType bufferSchema;

    public MergeArrayVariablesAggrUDF() {
        inputSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("index", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.StringType, true)
        ));

        this.inputSchema = inputSchema;
        this.bufferSchema = bufferSchema;
    }

    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    }

    @Override
    public Object evaluate(Row buffer) {
        return null;
    }
}
