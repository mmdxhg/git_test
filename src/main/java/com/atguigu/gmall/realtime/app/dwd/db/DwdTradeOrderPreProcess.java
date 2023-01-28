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

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
            3, Time.days(1),Time.minutes(3L)
                )
        );
        System.setProperty("HADOOP_USER_NAME","root");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));


        tableEnv.executeSql(
                "create table topic_db(\n" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`type` string,\n" +
                        "`data` Map<string,string>,\n" +
                        "`old` Map<string,string>,\n" +
                        "`proc_time` as PROCTIME(),\n" +
                        "`ts` string\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db"
                            ,"dwd_trade_order_pre_process")
        );

        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts od_ts,\n" +
                "proc_time\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");

        tableEnv.createTemporaryView("order_detail",orderDetail);

        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['order_status'] order_status,\n" +
                "`type`,\n" +
                "`old`,\n" +
                "ts oi_ts\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and (`type` = 'insert' or `type` = 'update')");

        tableEnv.createTemporaryView("order_info",orderInfo);

        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");

        tableEnv.createTemporaryView("order_detail_activity",orderDetailActivity);

        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");

        tableEnv.createTemporaryView("order_detail_coupon",orderDetailCoupon);

        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());

        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oi.user_id,\n" +
                "oi.order_status,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oi.operate_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "dic.dic_name source_type_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oi.`type`,\n" +
                "oi.`old`,\n" +
                "od.od_ts,\n" +
                "oi.oi_ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od \n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "left join `base_dic` for system_time as of od.proc_time as dic\n" +
                "on od.source_type = dic.dic_code");

        tableEnv.createTemporaryView("result_table",resultTable);

        tableEnv.executeSql(
                "create table dwd_trade_order_pre_process(\n" +
                        "id string,\n" +
                        "order_id string,\n" +
                        "user_id string,\n" +
                        "order_status string,\n" +
                        "sku_id string,\n" +
                        "sku_name string,\n" +
                        "province_id string,\n" +
                        "activity_id string,\n" +
                        "activity_rule_id string,\n" +
                        "coupon_id string,\n" +
                        "date_id string,\n" +
                        "create_time string,\n" +
                        "operate_date_id string,\n" +
                        "operate_time string,\n" +
                        "source_id string,\n" +
                        "source_type string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "split_original_amount string,\n" +
                        "split_activity_amount string,\n" +
                        "split_coupon_amount string,\n" +
                        "split_total_amount string,\n" +
                        "`type` string,\n" +
                        "`old` map<string,string>,\n" +
                        "od_ts string,\n" +
                        "oi_ts string,\n" +
                        "row_op_ts timestamp_ltz(3),\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL(
                                "dwd_tarde_order_pre_process"
                )
        );


        tableEnv.executeSql(
                "insert into dwd_trade_order_pre_process select * from result_table"
        ).print();


        env.execute();
    }
}
