package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class MyPhoenixSink  implements SinkFunction<JSONObject>{

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sinkTable = value.getString("sinkTable");

        value.remove("sinkTable");

        PhoenixUtil.insertValues(sinkTable,value);
    }
}
