package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.mysql.cj.xdevapi.Table;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject,String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public MyBroadcastFunction(MapStateDescriptor<String,TableProcess> mapState) {
        this.mapStateDescriptor = mapState;
    }

    //为了能够在广播流进入到程序的时候 ， 就创建出对应的phoenix 的 数据表
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String sourceTableName = value.getString("table");
        TableProcess tableProcess = broadcastState.get(sourceTableName);

        if (tableProcess != null){
            JSONObject data = value.getJSONObject("data");
            String sinkTable = tableProcess.getSinkTable();

            String sinkColumns = tableProcess.getSinkColumns();
            filter(data,sinkColumns);

            data.put("sinkTable",sinkTable);

            out.collect(data);
        }
    }

    private void filter(JSONObject data, String sinkColumns) {
        data.entrySet().removeIf(d -> !sinkColumns.contains(d.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        TableProcess config = JSON.parseObject(value).getObject("after", TableProcess.class);


        String sinkColumns = config.getSinkColumns();
        String sinkExtend = config.getSinkExtend();
        String sourceTable = config.getSourceTable();
        String sinkTable = config.getSinkTable();
        String sinkPk = config.getSinkPk();

        broadcastState.put(sourceTable,config);

        checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        /* 建表语句 ：
        CREATE TABLE IF NOT EXISTS GMALL2022_REALTIME_TEST.test
        (
        	id varchar PRIMARY KEY,
        	name varchar
        )
        */
        StringBuilder sql = new StringBuilder();

        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + sinkTable+ "(\n");

        String[] split = sinkColumns.split(",");
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }
        for (int i = 0; i < split.length; i++) {
            sql.append(split[i] + " varchar");
            if (split[i].equals(sinkPk)){
                sql.append(" primary key");
            }
            if (i < split.length - 1){
                sql.append(" ,\n");
            }
        }

        sql.append(")");
        sql.append(sinkExtend);

        String createTableSql = sql.toString();
        System.out.println(createTableSql);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createTableSql);
            preparedStatement.execute();
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
            System.out.println("建表语句：-- > " + createTableSql + "执行异常");
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                }catch (SQLException sqlException){
                    sqlException.printStackTrace();
                    throw  new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }
}