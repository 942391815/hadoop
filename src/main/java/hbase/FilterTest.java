package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * todo 过滤器深入理解  hbase 基于数值的过滤，分页PageFilter  自定义过滤器 hbase 协处理器
 *
 * <p/>
 * create 'user','info'
 * Created by qiaogu on 2017/2/25.
 */
public class FilterTest {
    public static void main(String[] args) throws Exception {
        Connection connection = HbaseClient.getConnection();
//        testQualiFiler(connection);
//        testValueFilter(connection);
//        testDependentColumnFilter(connection);
//        testPrefixFilter(connection);
        testFilterList(connection);

    }

    public static void testFilterList(Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf("user"));

        FilterList filterList = new FilterList();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),  //列族
                Bytes.toBytes("age"),  //列名
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("249"));

        filterList.addFilter(new PrefixFilter(Bytes.toBytes("lisi9")));
        filterList.addFilter(singleColumnValueFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        printMessage(table.getScanner(scan));
    }

    /**
     * 前缀过滤
     *
     * @param connection
     * @throws Exception
     */
    public static void testPrefixFilter(Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        PrefixFilter prefix = new PrefixFilter(Bytes.toBytes("lisi9"));
        scan.setFilter(prefix);

        printMessage(table.getScanner(scan));
    }

    /**
     * 参考列过滤器
     *
     * @param connection
     * @throws Exception
     */
    public static void testDependentColumnFilter(Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes("info"), Bytes.toBytes("age"));
        scan.setFilter(dependentColumnFilter);
        printMessage(table.getScanner(scan));

    }

    /**
     * 查找age 等于240的数据
     *
     * @param connection
     * @throws Exception
     */
    public static void testValueFilter(Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),  //列族
                Bytes.toBytes("age"),  //列名
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("240"));
        scan.setFilter(singleColumnValueFilter);
        printMessage(table.getScanner(scan));

    }

    /**
     * 列过滤器
     *
     * @param connection
     * @throws Exception
     */
    public static void testQualiFiler(Connection connection) throws Exception {
        Table user = connection.getTable(TableName.valueOf("user"));
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("age")));
        Scan scan = new Scan();
        scan.setFilter(qualifierFilter);

        ResultScanner scanner = user.getScanner(scan);
        for (Result res : scanner) {
            System.out.println(res);
        }
    }

    public static void testRowFilter(Connection connection) throws Exception {
        Scan scan = new Scan();
        Table user = connection.getTable(TableName.valueOf("user"));
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator((Bytes.toBytes("lisi24"))));
        scan.setFilter(rowFilter);

        ResultScanner scanner = user.getScanner(scan);
        for (Result res : scanner) {
            System.out.println(res);
        }
    }

    public static void printMessage(ResultScanner scanner) {
        for (Result res : scanner) {
//            List<Cell> columnCells = res.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age"));
//            for (Cell each:columnCells){
//                System.out.println(Bytes.toString(each.getValueArray()));
//            }
            System.out.println(res+"-----"+res.getColumnCells(Bytes.toBytes("info"),Bytes.toBytes("age")));
        }
    }
}
