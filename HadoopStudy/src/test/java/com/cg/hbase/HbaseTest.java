package com.cg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Cheng Guang on 2016/10/8.
 */
public class HbaseTest {

    // 声明静态配置
    private static Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    // 添加一条数据
    @Test
    public void addRow() throws IOException {

        String tableName = "blog_user";
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        String rowKey = "www.aboutyun.com";
        Put put = new Put(Bytes.toBytes(rowKey));

        String familyName = "userInfo";
        String[] columns = new String[] { "user_Password" };
        String[] values = new String[] { "654321" };
        for (int i = 0; i < columns.length; i++) {
            put.add(Bytes.toBytes(familyName),
                    Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    @Test
    public void getResult() throws IOException {
        String tableName = "blog_user";
        String rowKey = "www.aboutyun.com";
        String[] columns = new String[] { "user_Name", "user_Password", "user_Sex" };

        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        //        Connection connection = ConnectionFactory.createConnection(conf);
        //        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        String[] value = new String[columns.length];
        for (KeyValue kv : result.list()) {
            System.out.print(Bytes.toString(kv.getFamily())+":" + Bytes.toString(kv.getQualifier()) + " = ");
            System.out.println(Bytes.toString(kv.getValue()));

            //            for (int i = 0; i < columns.length; i++) {
            //                System.out.println("Qualifier:" + Bytes.toString(kv.getQualifier()));
            //                System.out.println(columns[i]);
            //                if (Bytes.toString(kv.getQualifier()).equals(columns[i])) {
            //                    value[i] = Bytes.toString(kv.getValue());
            //                    break;
            //                }
            //            }
        }
    }

    @Test
    public void getResultScan() throws IOException {
        String tableName = "blog_user";
        String start_rowkey = "www.aboutyun.com";
        String stop_rowkey = "www.aboutyun.com";
        String[] columns = new String[] { "user_Name", "user_Password", "user_Sex" };

        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        //范围查询
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                String[] value = new String[columns.length];
                for (KeyValue kv : r.list()) {
                    for (int i = 0; i < columns.length; i++) {
                        System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                        System.out.println(columns[i]);
                        if (Bytes.toString(kv.getQualifier()).equals(columns[i])) {
                            value[i] = Bytes.toString(kv.getValue());
                            break;
                        }
                    }
                }
            }
        } finally {
            rs.close();
        }
    }

    //    public void insertHbaseFir(List<AgentScoreModel> list) {
    //        try {
    //            HTableInterface table = conn.getTable(Constants.AGENTSCORE);
    //            // 批处理
    //            List<Put> puts = new ArrayList<Put>();
    //            for (AgentScoreModel model : list) {
    //                Put put = new Put(Bytes.toBytes(model.getData_dt() + ":" + model.getHouseId()));
    //
    //                put.add(Bytes.toBytes("info"), Bytes.toBytes("moreinfo"),
    //                        Bytes.toBytes(model.getMoreInfo()));
    //
    //                // 同步写入日志，影响写入的性能，但能保证数据安全
    //                put.setDurability(Durability.SYNC_WAL);
    //                puts.add(put);
    //                if (puts.size() >= 1000) {
    //                    table.put(puts);
    //                    table.flushCommits();
    //                    puts.clear();
    //                }
    //            }
    //            // 防止最后情况下，不足1000的情况下
    //            if(puts.size() > 0){
    //                table.put(puts);
    //                table.flushCommits();
    //                puts.clear();
    //            }
    ////            log.info("第一张表的数据插入hbase数据完成！！！");
    ////            closeTable(table);
    //        } catch (IOException e) {
    ////            log.error("table operator error!");
    //            e.printStackTrace();
    //        }
    //    }

}
