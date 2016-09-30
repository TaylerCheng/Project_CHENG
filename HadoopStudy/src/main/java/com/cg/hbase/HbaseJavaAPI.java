package com.cg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2016/9/29.
 */
public class HbaseJavaAPI {

    // 声明静态配置
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop-101-42,hadoop-101-43,hadoop-101-44");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    //判断表是否存在
    public static boolean isExist(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        return hAdmin.tableExists(tableName);
    }

    // 添加一条数据
    public static void addRow(String rowKey, String tableName, String familyName,
                              String[] column, String[] value) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < column.length; i++) {
            put.add(Bytes.toBytes(familyName),
                    Bytes.toBytes(column[i]), Bytes.toBytes(value[i]));
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    //rowkey的转换
    public static String getrowKey(String[] value) {
        String rowkey_part1 = MD5Util.md5sum(value[0]);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = null;
        try {
            date = sdf.parse(value[4]);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String rowkey_part2 = new String();
        if (value[7] == "被叫") {
            rowkey_part2 = "0";
        } else {
            rowkey_part2 = "1";
        }

        String rowkey_part3 = sdf2.format(date);
//        System.out.println(rowkey_part1);
//        System.out.println(rowkey_part2);
//        System.out.println(rowkey_part3);
        String rowkey = rowkey_part1 + rowkey_part2 + rowkey_part3;
//        System.out.println(rowkey);
        return rowkey;
    }

    // 删除一条(行)数据
    public static void delRow(String tableName, String row) throws Exception {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    //根据rowkey行查询
    public static String[] getResult(String tableName, String rowKey, String[] column)
            throws IOException {
        String[] value = new String[column.length];
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            for (int i = 0; i < column.length; i++) {
//                System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//                System.out.println(column[i]);
                if (Bytes.toString(kv.getQualifier()).equals(column[i])) {
                value[i] = Bytes.toString(kv.getValue());
                break;
                }
            }
        }
        return value;
    }

    //根据rowkey行range查询
    public static void getResultScan(String tableName, String[] column,String start_rowkey,
                                      String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                String[] value = new String[column.length];
                for (KeyValue kv : r.list()) {
                    for (int i = 0; i < column.length; i++) {
//                      System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//                      System.out.println(column[i]);
                        if (Bytes.toString(kv.getQualifier()).equals(column[i])) {
                            value[i] = Bytes.toString(kv.getValue());
                            break;
                        }
                    }
                }

                for (int i = 0; i < value.length; i++) {
                    System.out.print(column[i] + "  ");
                }
                System.out.println();
                for (int i = 0; i < value.length; i++) {
                    System.out.print(value[i] + " ");
                }
                System.out.println();

            }
        } finally {
            rs.close();
        }
    }


}
