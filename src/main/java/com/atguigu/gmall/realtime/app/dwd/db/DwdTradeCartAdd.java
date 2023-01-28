package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MySqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME","root");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        String topic = "topic_db";
        String groupId = "dwd_trade_cart_add";

        tableEnv.executeSql(
                "create table topic_db(\n" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`type` string,\n" +
                        "`data` map<string,string>,\n" +
                        "`old` map<string,string>,\n" +
                        "`ts` string\n" +
                        "`proc_time` as PROCTIME()\n" +
                        ")" + KafkaUtil.getKafkaDDL(topic,groupId)
        );

        Table cartAddTable = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['source_id'] source_id,\n" +
                        "data['source_type'] source_type,\n" +
                        "if(type = 'insert',\n" +
                        "data['sku_num'],cast((cast(data['sku_num'] as int) - \n" +
                        "cast(old['sku_num'] as int)) as string)) sku_num,\n" +
                        "ts,\n" +
                        "proc_time\n" +
                        "from topic_db\n" +
                        "where table = cart_info\n" +
                        "and (type = insert \n" +
                        "or (type = update \n" +
                        "and old['sku_num'] is not null \n" +
                        "and cast(data['sku_num'] as int) > cast(old['sku_num'] as int )))"
        );

        tableEnv.createTemporaryView("cart_add",cartAddTable);

        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());

        Table resultTable = tableEnv.sqlQuery(
                "select\n" +
                        " cadd.id,\n" +
                        " user_id,\n" +
                        " sku_id,\n" +
                        " source_id,\n" +
                        " source_type,\n" +
                        " dic_name source_type_name,\n" +
                        " sku_num,\n" +
                        " ts\n" +
                        "from cart_add cadd\n" +
                        "left join base_dic for system_time as of cadd.proc_time as dic\n" +
                        "on cadd.source_type = dic.dic_code"
        );
        tableEnv.createTemporaryView("result_table",resultTable);
        tableEnv.executeSql(
                "create table dwd_trade_cart_add(\n" +
                        "id string,\n" +
                        "user_id string,\n" +
                        "sku_id string,\n" +
                        "source_id string,\n" +
                        "source_type_code string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "ts string,\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add")
        );

        tableEnv.executeSql(
                "insert into dwd_trade_cart_add select * from result_table"
        );

        env.execute();
    }
}
