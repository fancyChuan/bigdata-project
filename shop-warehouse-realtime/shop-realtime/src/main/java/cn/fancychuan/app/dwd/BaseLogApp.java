package cn.fancychuan.app.dwd;

import cn.fancychuan.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 数据流：web/app -> nginx -> spring boot -> kafka(ods) -> Flink:BaseLogApp -> kafka(dwd)
 * 程 序：mockLog -> nginx -> shop-logger.sh -> kafka ->  Flink:BaseLogApp -> kafka
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从kafka消费ods_base_log的数据，并创建流
        String topic = "ods_base_log";
        String groupId = "log_cousumers";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // 3. 将字符串转为json对象
        // 这里要注意检查脏数据，而脏数据其实也是需要保留的，由此得使用process，用map的话做不到
        OutputTag<String> outputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            context.output(outputTag, s);
                        }
                    }
                });
        jsonObjStream.getSideOutput(outputTag).print("Dirty>>>>");

        // 4. 新老用户的判断，用状态编程
        // 跟进mid来判断
        KeyedStream<JSONObject, String> jsonObjDS = jsonObjStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取数据中的"is_new"标识
                String isNew = value.getJSONObject("common").getString("is_nuew");
                if ("1".equals(isNew)) {
                    String state = valueState.value();
                    if (state != null) {
                        //修改isNew标记
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("1");
                    }
                }
                return value;
            }
        });

        // 5. 分流：页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取启动日志字段
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    out.collect(value.toJSONString());

                    //取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {

                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //添加页面id
                            display.put("page_id", pageId);

                            //将输出写出到曝光侧输出流
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // 6. 提取测输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 7. 将三种输出流进行打印，并写入三个kafka topic
        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        env.execute("BaseLogApp");
    }
}
