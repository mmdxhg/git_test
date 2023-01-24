package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyPhoenixSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.net.ConnectException;
import java.sql.*;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

        private static Connection connection;

        public static void  initializeConnection(){
            try {
                Class.forName(GmallConfig.PHOENIX_DRIVER);
                connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
                connection.setSchema(GmallConfig.HBASE_SCHEMA);
            }catch (ClassNotFoundException classNotFoundException){
                System.out.println("注册驱动异常");
                classNotFoundException.printStackTrace();
            }catch (SQLException sqlException){
                System.out.println("获取连接对象异常");
                sqlException.printStackTrace();
             }
        }

    public static void insertValues(String sinkTable,JSONObject data){
        if (connection == null){
            synchronized (MyPhoenixSink.class){
                initializeConnection();
            }
        }

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        String columnsStr = StringUtils.join(columns, ",");
        String valuesStr = StringUtils.join(values, "','");

        String sql
                = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + columnsStr + ") " + "values " + "('" + valuesStr + "')";

        PreparedStatement preparedStatement = null;

        try {
            System.out.println("要执行输出的语句是：" + sql);
            preparedStatement = connection.prepareStatement(sql);
            connection.commit();
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
            throw new RuntimeException("数据库操作对象获取或执行sql语句异常");
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
