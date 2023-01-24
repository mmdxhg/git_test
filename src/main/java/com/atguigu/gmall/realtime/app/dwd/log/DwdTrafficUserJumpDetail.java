package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1),Time.minutes(1)));
        System.setProperty("HADOOP_USER_NAME","root");

        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";

        DataStreamSource<String> source = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> mappedDStream
                = source.map(data -> JSONObject.parseObject(data)).returns(JSONObject.class);

        SingleOutputStreamOperator<JSONObject> withWaterMarkerDStream = mappedDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        KeyedStream<JSONObject, String> keyedStream = withWaterMarkerDStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        Pattern<JSONObject, JSONObject> partten = Pattern.<JSONObject>begin("first").where(
                new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }
        ).next("second").where(
                new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }
        ).within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));


        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, partten);

        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {
        };

        SingleOutputStreamOperator<JSONObject> flatSelectStream = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                JSONObject element = map.get("first").get(0);
                collector.collect(element);
            }
        }, new PatternFlatSelectFunction<JSONObject, JSONObject>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                JSONObject element = map.get("first").get(0);

                collector.collect(element);
            }
        });

        DataStream<JSONObject> timeOutDStream = flatSelectStream.getSideOutput(timeoutTag);


        DataStream<JSONObject> unionDStream = timeOutDStream.union(flatSelectStream);

        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDStream.map(data->data.toJSONString()).returns(String.class).addSink(
                KafkaUtil.getKafkaProducer(targetTopic)
        );

        env.execute();
    }
}
