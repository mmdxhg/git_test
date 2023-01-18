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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.读取maxwell输出给 topic 的 业务数据库的 变化数据
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        DataStreamSource<String> streamSource = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));


        //2.进行格式转换 然后 进行过滤
        SingleOutputStreamOperator<JSONObject> mapped = streamSource.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        return JSON.parseObject(s);
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> filteredDStream = mapped.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        try {
                            jsonObject.getJSONObject("data");
                            if (jsonObject.getString("type").equals("bootstrap-start")
                                    || jsonObject.getString("type").equals("bootstrap-complete")) {
                                return false;
                            }
                            return true;
                        } catch (JSONException e) {
                            return false;
                        }
                    }
                }
        );

        // 由于要对 数据进行分流 所以需要使用到 flink cdc 读取配置文件作为广播流进行操作 在此之前，需要在hbase 中创建对应的命名空间
        // todo 读取mysql 数据库中 的 配置文件表 然后形成广播流
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlDStream =
                env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");


        //配置广播流
        MapStateDescriptor<String, TableProcess> broadcastState = new MapStateDescriptor<>(
                "broadcast_stream", String.class, TableProcess.class
        );
        BroadcastStream<String> broadcast = mysqlDStream.broadcast(broadcastState);



        //todo 将常规流与 广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connect = filteredDStream.connect(broadcast);


        SingleOutputStreamOperator<JSONObject> process = connect.process(new MyBroadcastFunction(broadcastState));

        process.addSink(new MyPhoenixSink());


        env.execute();
    }
}