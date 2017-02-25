package hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by qiaogu on 2017/2/20.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Connection connection = HbaseClient.getConnection();
        long start = System.currentTimeMillis();
//        getCellByRow(connection);
//        insert(connection);
//        checkAndPut(connection);
//        testScanner(connection);
        long end = System.currentTimeMillis();
        System.out.println(end - start);
//        11776
    }

    public static void testScanner(Connection connection) throws Exception {

        try {
            Table user = connection.getTable(TableName.valueOf("user"));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("lisi"));
            scan.setStopRow(Bytes.toBytes("lisi99"));
            ResultScanner scanner = user.getScanner(scan);
            Thread.sleep(HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD + 10000);
            for (Result each : scanner) {
                System.out.println(Bytes.toString(each.getRow()));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static void getCellByRow(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        Get get = new Get(Bytes.toBytes("lisi"));
//        get.setMaxVersions(3);//获取多个版本
        Result result = table.get(get);

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("age"));


        byte[] row = result.getRow();
        System.out.println(Bytes.toString(row));
    }

    private static void println(Object obj) {
        System.out.println(obj);
    }

    private static void insert(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        List<Put> list = new ArrayList<Put>();
        for (int i = 0; i < 100; i++) {
            Put put = new Put(Bytes.toBytes("lisi" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi" + i));
            list.add(put);
        }
        table.put(list);
        table.close();
        connection.close();
    }

    private static void checkAndPut(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        Put put = new Put(Bytes.toBytes("lisi9998"));
//         lisi9998                                                            column=f1:age9998, timestamp=1487855172236, value=249998
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age9998"), Bytes.toBytes("121212121212"));
        boolean b = table.checkAndPut(Bytes.toBytes("lisi9998"), Bytes.toBytes("f1"), Bytes.toBytes("age9998"), Bytes.toBytes("249998"), put);
        System.out.println(b);

        table.close();
        connection.close();
    }
}
