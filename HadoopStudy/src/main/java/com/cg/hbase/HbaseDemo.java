package com.cg.hbase;

/**
 * Created by Administrator on 2016/9/28.
 */
public class HbaseDemo {

    public static void main(String[] args) throws Exception {

/*        //对表p2p_cust_addr_list操作
        String tableName = "p2p_cust_addr_list";
        String familyName = "info";
        String[] column = { "cust_id", "mobile", "create_time", "mobile_name", "mobile_no", "city_name", "city_code", "update_time" };
        String[] value = { "25212778", "60013805824579", "2016-03-02 11:05:16","卢焕江","13805824579","","","2016-02-29 03:25:36" };
        String rowKey = MD5Util.md5sum(value[0]);
        //插入一条数据
        HbaseJavaAPI.addRow(rowKey,tableName,familyName,column,value);
        //根据rowKey得到一条数据
        String[] value_back = HbaseJavaAPI.getResult(tableName,rowKey,column);
        for (int i = 0; i < value.length; i++ ) {
            System.out.println(column[i] + "  " + value_back[i]);
        }
        //根据rowKey删除一条数据
        HbaseJavaAPI.delRow(tableName,rowKey);
        //判断表是否存在
        System.out.println(HbaseJavaAPI.isExist("p2p_cust_addr_list"));*/

        //对表jxl_phone_call_records操作
        String tableName = "jxl_phone_call_records";
        String familyName = "info";
        String[] column = {"primary_id", "cell_phone", "other_cell_phone", "call_place", "start_time", "use_time", "call_type", "init_type", "subtotal", "update_time"};
        String[] value = {"1934", "18906967868", "18559456947", "", "2015-11-20 17:39:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value1 = {"1934", "18906967868", "18559456947", "", "2015-12-20 17:39:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value2 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:39:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value3 = {"1934", "18906967868", "18559456947", "", "2016-01-20 18:39:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value4 = {"1934", "18906967868", "18559456947", "", "2016-01-20 19:39:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value5 = {"1934", "18906967868", "18559456947", "", "2016-01-20 19:40:59", "55", "", "主叫", "0", "2016-04-08 09:42:58"};
        String[] value6 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:41:59", "55", "", "主叫", "0", "2016-04-08 09:42:58"};
        String[] value7 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:42:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value8 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:43:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value9 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:44:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};
        String[] value10 = {"1934", "18906967868", "18559456947", "", "2016-01-20 17:45:59", "55", "", "被叫", "0", "2016-04-08 09:42:58"};

        //插入一批数据
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value), tableName, familyName, column, value);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value1), tableName, familyName, column, value1);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value2), tableName, familyName, column, value2);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value3), tableName, familyName, column, value3);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value4), tableName, familyName, column, value4);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value5), tableName, familyName, column, value5);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value6), tableName, familyName, column, value6);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value7), tableName, familyName, column, value7);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value8), tableName, familyName, column, value8);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value9), tableName, familyName, column, value9);
        HbaseJavaAPI.addRow(HbaseJavaAPI.getrowKey(value10), tableName, familyName, column, value10);

        //根据rowkey查询
        String[] value_back = HbaseJavaAPI.getResult(tableName, HbaseJavaAPI.getrowKey(value), column);
        for (int i = 0; i < value.length; i++) {
            System.out.println(column[i] + "  " + value_back[i]);
        }

        String startRowKey = "286544710B2E7EBE201511";
        String stopRowKey = "286544710B2E7EBE201612";
        HbaseJavaAPI.getResultScan(tableName,column, startRowKey, stopRowKey);

    }
}
