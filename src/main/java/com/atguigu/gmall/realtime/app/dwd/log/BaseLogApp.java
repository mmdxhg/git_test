package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(
                60 * 1000L
        );
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(3, TimeUnit.DAYS),Time.of(1L,TimeUnit.MINUTES)
        ));

        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/gmall/ck"
        );
        System.setProperty("HADOOP_USER_NAME","root");

        //todo read data steam from kafka topic_log
        String topic = "topic_log";
        String groupId = "base_log_consumer";

        DataStreamSource<String> source = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId)).setParallelism(2);


        //数据清洗 结果转换
        OutputTag<String> dirtyStreamTag = new OutputTag<String>("dirtyStream"){};

        SingleOutputStreamOperator<String> cleanedStream = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    System.out.println(value);
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(value);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(dirtyStreamTag, value);
                }
            }
        });

        DataStream<String> dirtyStream = cleanedStream.getSideOutput(dirtyStreamTag);

        String dirtyTopic = "dirty_data";

        dirtyStream.addSink(KafkaUtil.getKafkaProducer(dirtyTopic));

        SingleOutputStreamOperator<JSONObject> streamOperator = cleanedStream.map(data -> JSONObject.parseObject(data)).returns(JSONObject.class);

        streamOperator.print("streamOperator>>>>");
        //对新老顾客的状态进行修改
        KeyedStream<JSONObject, String> keyedByMidDStream = streamOperator.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> fixedStream  = keyedByMidDStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>(
                                        "last_visit_time", TypeInformation.of(String.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = value.getJSONObject("common").getString("isNew");

                        String firstViewDt = valueState.value();
                        Long ts = value.getLong("ts");
                        String dt = DateFormatUtil.toDate(ts);

                        if ("1".equals(isNew)) {
                            if (firstViewDt == null) {
                                valueState.update(dt);
                            } else {
                                if (firstViewDt.equals(dt)) {
                                    isNew = "0";

                                    value.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (firstViewDt == null) {
                                valueState.update(DateFormatUtil.toDate(ts - (1000 * 60 * 24)));
                            }
                        }
                        out.collect(value);
                    }
                }
        );

        //todo 分流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("errorTag") {
        };

        SingleOutputStreamOperator<String> separatedStream  = fixedStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        JSONObject error = value.getJSONObject("err");

                        if (error != null) {
                            ctx.output(errorTag, value.toJSONString());
                        }

                        value.remove("err");

                        JSONObject start = value.getJSONObject("start");

                        if (start != null) {
                            ctx.output(startTag, value.toJSONString());
                        } else {
                            JSONObject page = value.getJSONObject("page");
                            JSONObject common = value.getJSONObject("common");
                            Long ts = value.getLong("ts");

                            JSONArray displays = value.getJSONArray("displays");

                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    JSONObject displayObj = new JSONObject();

                                    displayObj.put("display", display);
                                    displayObj.put("common", common);
                                    displayObj.put("page", page);
                                    displayObj.put("ts", ts);
                                    ctx.output(displayTag, displayObj.toJSONString());

                                }
                            }

                            JSONArray actions = value.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject actionObj = new JSONObject();

                                    JSONObject action = actions.getJSONObject(i);

                                    actionObj.put("action", action);
                                    actionObj.put("common", common);
                                    actionObj.put("page", page);
                                    actionObj.put("ts", ts);
                                    ctx.output(actionTag, actionObj.toJSONString());

                                }
                            }

                            value.remove("displays");
                            value.remove("actions");
                            out.collect(value.toJSONString());
                        }
                    }
                }
        );


        DataStream<String> startDS = separatedStream.getSideOutput(startTag);
        DataStream<String> displayDS = separatedStream.getSideOutput(displayTag);
        DataStream<String> actionDS = separatedStream.getSideOutput(actionTag);
        DataStream<String> errorDS = separatedStream.getSideOutput(errorTag);

        separatedStream.print("separatedDStream>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        separatedStream.addSink(KafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(KafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(KafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(KafkaUtil.getKafkaProducer(error_topic));


        env.execute();
    }
}
