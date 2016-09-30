package com.cg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by Administrator on 2016/9/29.
 */
public class New112HbaseJavaAPI {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop-101-42,hadoop-101-43,hadoop-101-44");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.master", "192.168.101.41:16020");


        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String tableName, String[] cols) throws IOException {

        init();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }



    public static void main(String[] args) throws IOException {
//        createTable("p2p_cust_addr_list1", new String[]{"info"});
        listTables();
    }

}
