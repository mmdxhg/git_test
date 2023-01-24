package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.checkerframework.checker.units.qual.K;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME","root");

        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";


        DataStreamSource<String> source = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> mappedDStream
                = source.map(data -> JSONObject.parseObject(data)).returns(JSONObject.class);

        SingleOutputStreamOperator<JSONObject> filterDStream = mappedDStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        });

        KeyedStream<JSONObject, String> keyedStream = filterDStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<String> visitDate = new ValueStateDescriptor<>("last_visit_date", String.class);
                visitDate.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastVisitDt = getRuntimeContext().getState(visitDate);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                String lastDateValue = lastVisitDt.value();

                if (lastDateValue == null && !lastDateValue.equals(currentDate)) {
                    lastVisitDt.update(currentDate);
                    return true;
                }
                return false;
            }
        });

        String targetTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);

        filterDStream.map(data -> data.toJSONString()).returns(String.class).addSink(kafkaProducer);

        env.execute();
    }
}
