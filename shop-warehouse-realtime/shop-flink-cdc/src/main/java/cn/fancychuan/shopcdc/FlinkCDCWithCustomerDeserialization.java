package cn.fancychuan.shopcdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hphost").port(3307)
                .username("root").password("123456")
                .databaseList("gmall-flink")
                // 可选配置，不配置时默认为整库。注意格式是：db.table
                .tableList("gmall-flink.base_trademark")
                // 反序列化器，其实 StringDebeziumDeserializationSchema 封装得不是特别好
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> dataStream = env.addSource(sourceFunction);

        dataStream.print();

        env.execute("custom Deserialization");
    }
}
