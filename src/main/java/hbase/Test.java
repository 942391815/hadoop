package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

import java.io.IOException;
import java.util.List;

/**
 * Created by qiaogu on 2017/2/20.
 */
public class Test {
    public static void main(String[] args) throws Exception{
        Connection connection = HbaseClient.getConnection();
        getCellByRow(connection);
    }

    private static void getCellByRow(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        Get get = new Get(Bytes.toBytes("lisi"));
//        get.setMaxVersions(2);//获取多个版本
        Result result = table.get(get);
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("age"));
        for (Cell each:columnCells){
            println("RowName: " + new String(CellUtil.cloneRow(each)) + " ");
            println("Timetamp: " + each.getTimestamp() + " ");
            println("column Family: " + new String(CellUtil.cloneFamily(each)) + " ");
            println("row Name: " + new String(CellUtil.cloneQualifier(each)) + " ");
            println("value: " + new String(CellUtil.cloneValue(each)) + " ");
        }
    }

    private static void println(Object obj) {
        System.out.println(obj);
    }
    private static void insert(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        Put put = new Put(Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes("24"));
        table.put(put);
        table.close();
        connection.close();
    }
}
