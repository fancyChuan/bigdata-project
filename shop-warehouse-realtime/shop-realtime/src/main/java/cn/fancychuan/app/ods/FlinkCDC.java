package cn.fancychuan.app.ods;

import cn.fancychuan.app.function.CustomerDeserialization;
import cn.fancychuan.util.MyKafkaUtil;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hphost").port(3307)
                .username("root").password("123456")
                .databaseList("gmall-flink")
                // 可选配置，不配置时默认为整库。注意格式是：db.table
//                .tableList("gmall-flink.base_trademark")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        dataStreamSource.print();
        // 将数据写入kafka
        dataStreamSource.addSink(MyKafkaUtil.getKafkaProducer("ods_base_db"));

        env.execute("ODS Flink CDC");
    }
}
