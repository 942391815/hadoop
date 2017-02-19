/**
 * Created by qiaogu on 2017/2/19.
 * hbase 的version 在创建版本的时候修改version
 */
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTestCase {
    /**
     * @param args
     */
    public static void main(String[] args) {
        String tableName = "testtable1";
        String columnFamily = "colfam1";
        try {

//            if (true == HBaseTestCase.delete(tableName)) {
//                System.out.println("Delete Table " + tableName + " success!");
//
//            }
//
//            HBaseTestCase.create(tableName, columnFamily);
//            HBaseTestCase.put(tableName, "row1", columnFamily, "column1",
//                    "data1111112121asasasd");
//            HBaseTestCase.put(tableName, "row2", columnFamily, "column2",
//                    "data2");
//            HBaseTestCase.put(tableName, "row3", columnFamily, "column3",
//                    "data3");
//            HBaseTestCase.put(tableName, "row4", columnFamily, "column4",
//                    "data4");
//            HBaseTestCase.put(tableName, "row5", columnFamily, "column5",
//                    "data5");

            HBaseTestCase.get(tableName, "row1");

//            HBaseTestCase.scan(tableName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Configuration cfg = HBaseConfiguration.create();
    static {
        cfg.set("hbase.zookeeper.quorum", "192.168.0.253");
        cfg.set("hbase.rootdir", "hdfs://192.168.0.253:9000/hbase");
        cfg.set("hbase.master", "192.168.0.253");
//        System.setProperty("hadoop.home.dir","E:\\mytool\\hadoop-2.7.3");

//        System.out.println(cfg.get("hbase.master"));
    }

    public static void create(String tableName, String columnFamily)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + " exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println(tableName + " create successfully!");
        }
    }

    public static void put(String tablename, String row, String columnFamily,
                           String column, String data) throws Exception {

        HTable table = new HTable(cfg, tablename);
        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));

        table.put(put);

        System.out.println("put '" + row + "', '" + columnFamily + ":" + column
                + "', '" + data + "'");

    }

    public static void get(String tablename, String row) throws Exception {
        HTable table = new HTable(cfg, tablename);
        Get get = new Get(Bytes.toBytes("row-1")); // co GetExample-3-NewGet Create get with specific row.
        get.setMaxVersions();
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        Result result = table.get(get);
        List<KeyValue> list = result.list();
        for(final KeyValue kv:list){
            // System.out.println("value: "+ kv+ " str: "+Bytes.toString(kv.getValue()));
            System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                    Bytes.toString(kv.getRow()),
                    Bytes.toString(kv.getFamily()),
                    Bytes.toString(kv.getQualifier()),
                    Bytes.toString(kv.getValue()),
                    kv.getTimestamp()));
        }
    }

    public static void scan(String tableName) throws Exception {

        HTable table = new HTable(cfg, tableName);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan: " + r);

        }
    }

    public static boolean delete(String tableName) throws IOException {

        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}
