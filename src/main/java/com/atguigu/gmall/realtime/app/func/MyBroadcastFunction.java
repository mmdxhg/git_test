package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.mysql.cj.xdevapi.Table;
import javafx.scene.control.Tab;
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
import java.util.Set;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private  MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private static Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null){
            JSONObject data = value.getJSONObject("data");

            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumns(data,sinkColumns);

            data.put("sinkTable",sinkTable);
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf( d -> !sinkColumns.contains(d.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        TableProcess tableProcess = JSON.parseObject(value).getObject("after", TableProcess.class);
        String sourceTable = tableProcess.getSourceTable();
        String sinkTable = tableProcess.getSinkTable();
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkExtend = tableProcess.getSinkExtend();
        String sinkPk = tableProcess.getSinkPk();

        broadcastState.put(sourceTable,tableProcess);

        checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        //?????????????????????????????????????????????
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(\n" );

        String[] columns = sinkColumns.split(",");

        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i] + " varchar ");
            if (sinkPk.equals(columns[i])){
                sql.append(" primary key");
            }

            if (i < columns.length - 1){
                sql.append(", \n");
            }
        }

        sql.append(")");
        sql.append(sinkExtend);

        String createSQL = sql.toString();

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(createSQL);
            preparedStatement.execute();
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
            throw new RuntimeException("????????????\n" + createSQL + "\n????????????????????????");
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                }catch (SQLException sqlException){
                    sqlException.printStackTrace();
                    throw new RuntimeException("?????????????????????????????????");
                }
            }
        }
    }
}