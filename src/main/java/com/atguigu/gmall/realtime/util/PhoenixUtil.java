package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.net.ConnectException;
import java.sql.*;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    //1.定义数据库连接对象
    private static Connection conn;

    public static void initializeConnection(){
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        }catch (ClassNotFoundException classNotFoundException){
            classNotFoundException.printStackTrace();
            throw new RuntimeException("注册驱动异常");
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
            throw new RuntimeException("获取连接对象异常");
        }
    }

    public static void insertValues(String sinkTable, JSONObject data){
        if (conn == null){
            synchronized (PhoenixUtil.class){
                initializeConnection();
            }
        }

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        String allColumns = StringUtils.join(columns, ",");
        String allValues = StringUtils.join(values, "','");

        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + allColumns + ")" + " values ('" + allValues + "')";

        PreparedStatement preparedStatement = null;

        try {
            //System.out.println("当前插入语句为" + sql);
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
            conn.commit();
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
            throw new RuntimeException("数据库操作对象获取或执行异常");
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                }catch (SQLException sqlException){
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }
}
