package com.atguigu.gmall.realtime.app.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyBroadcastFunction;
import com.atguigu.gmall.realtime.app.func.MyPhoenixSink;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS),Time.of(3L,TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");

        System.setProperty("HADOOP_USER_NAME","root");

        //todo 2.获取主流任务
        String topic = "topic_db";
        String groupId = "dim_sink_app";


        DataStreamSource<String> topic_db_DStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        topic_db_DStream.print("kafka>>>");
        //todo 3.对主体数据进行数据的格式转变 然后再对脏数据进行清洗 脏数据发送到 侧输出流
        SingleOutputStreamOperator<JSONObject> mappedDStream = topic_db_DStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        try {
                            JSONObject value = JSON.parseObject(s);
                            return value;
                        } catch (JSONException jsonException) {
                            jsonException.printStackTrace();
                            return null;
                        }
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> filterDStream = mappedDStream.filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    jsonObject.getJSONObject("data");

                    if (jsonObject.getString("type").equals("bootstrap-start")
                            || jsonObject.getString("type").equals("bootstrap-complete")) {
                        return false;
                    }
                    return true;
                } catch (JSONException jsonException) {
                    return false;
                }
            }
        });

        //读取配置文件表中的数据，形成广播流
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .username("root")
                .password("123456")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> table_process = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "table_process");


        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process_status", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = table_process.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDStream.connect(broadcastStream);

        SingleOutputStreamOperator<JSONObject> processDStream
                = broadcastConnectedStream.process(new MyBroadcastFunction(mapStateDescriptor));


        processDStream.addSink(new MyPhoenixSink());

        env.execute();

    }
}