package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPhoenixSink implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sinkTable = value.getString("sinkTable");

        value.remove("sinkTable");

        PhoenixUtil.insertValues(sinkTable,value);
    }
}
