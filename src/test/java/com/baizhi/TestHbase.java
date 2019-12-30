package com.baizhi;

import com.baizhi.entity.EvalReport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

public class TestHbase {


    private Connection connection;

    @Before
    public void testHbase() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "SparkOnStandalone");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testPut() throws IOException {
        TableName tableName = TableName.valueOf("spark:t_evaluate");
        Table table = connection.getTable(tableName);
        Put put = new Put("test201912301510".getBytes());
        /*put.addColumn("info".getBytes(), "age".getBytes(), "25".getBytes());
        put.addColumn("info".getBytes(), "name".getBytes(), "Test20191230".getBytes());
        put.addColumn("info".getBytes(), "sex".getBytes(), "true".getBytes());*/
        put.addColumn("info".getBytes(), "applicationId".getBytes(), "1001".getBytes());
        table.put(put);
        table.close();
    }

    /**
     *
     */
    @Test
    public void testPut1() throws IOException {
        EvalReport report = new EvalReport();
        report.setApplicationId("APPTEST");
        report.setReportTime(new Date().getTime());
        report.setUserId("1001");
        report.setRegion("beijing");
        report.setLoginSequence("jfdsfkdfslfdfdjfslkfd");
        HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
        hashMap.put("login_count", true);
        hashMap.put("password", true);
        hashMap.put("login_habit", true);
        hashMap.put("input_feature", true);
        hashMap.put("region", true);
        hashMap.put("device", true);
        hashMap.put("speed", true);
        report.setReportItems(hashMap);


        TableName tableName = TableName.valueOf("spark:t_evaluate");
        Table table = connection.getTable(tableName);

        Put put = new Put("test".getBytes());
        put.addColumn("cf1".getBytes(), "applicationId".getBytes(), report.getApplicationId().getBytes());
        put.addColumn("cf1".getBytes(), "userId".getBytes(), report.getUserId().getBytes());
        put.addColumn("cf1".getBytes(), "region".getBytes(), report.getRegion().getBytes());
        put.addColumn("cf1".getBytes(), "loginSequence".getBytes(), report.getLoginSequence().getBytes());
        put.addColumn("cf1".getBytes(), "reportTime".getBytes(), String.valueOf(report.getReportTime()).getBytes());
        put.addColumn("cf1".getBytes(), "login_count".getBytes(), report.getReportItems().get("login_count").toString().getBytes());
        put.addColumn("cf1".getBytes(), "password".getBytes(), report.getReportItems().get("password").toString().getBytes());
        put.addColumn("cf1".getBytes(), "login_habit".getBytes(), report.getReportItems().get("login_habit").toString().getBytes());
        put.addColumn("cf1".getBytes(), "input_feature".getBytes(), report.getReportItems().get("input_feature").toString().getBytes());
        put.addColumn("cf1".getBytes(), "region".getBytes(), report.getReportItems().get("region").toString().getBytes());
        put.addColumn("cf1".getBytes(), "device".getBytes(), report.getReportItems().get("device").toString().getBytes());
        put.addColumn("cf1".getBytes(), "speed".getBytes(), report.getReportItems().get("speed").toString().getBytes());

        table.put(put);
        table.close();

    }

}
