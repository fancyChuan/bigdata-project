package cn.fancychuan.function;

import cn.fancychuan.bean.TableProcess;
import cn.fancychuan.common.GmallConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 对合并流进行处理
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 初始化连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 主流：需要完成的事情
     *  1.读取广播的配置数据
     *  2.过滤字段
     *  3.分流
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // table_process表的主键是 tablename和operate_type，因此这里的key要用这两个字段，并且用"-"连接
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.过滤字段（过滤出数据流中我们关心的字段）
            JSONObject data = value.getJSONObject("after");
            // 注意：结果这个函数处理之后，data里面的内容已经变化了
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //将输出表/主题信息写入Value中（也就是流的内容）
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //Kafka数据,写入主流
                collector.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //HBase数据,写入侧输出流
                readOnlyContext.output(objectOutputTag, value);
            }

        } else {
            System.out.println("该组合Key：" + key + "不存在！");
        }
    }

    /**
     * 广播流：需要完成的事情
     *  1.解析数据，将String转为TableProcess
     *  2.检查HBase表是否存在，不存在则建表
     *  3.写入状态，广播出去
     *
     *  value: {"db":", "tx":"", "before":{}, "after":{}, "type":""}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    /**
     * 建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                //判断是否为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }

                //判断是否为最后一个字段,如果不是,则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 过滤字段：只需要配置表中配置了的字段
     * @param data          {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
     * @param sinkColumns   id,tm_name
     *                      {"id":"11","tm_name":"atguigu"}  不关心的logo_url被过滤掉了
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }
}
