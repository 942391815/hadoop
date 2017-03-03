package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qiaogu on 2017/3/2.
 */
public class CountTest {
    public static void main(String[] args) throws Exception{
        final Connection connection = HbaseClient.getConnection();
        for(int i=0;i<100;i++){
            new Thread(new Runnable() {
                public void run() {
                    try {
                        testCount(connection);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        }


    }

    /**
     * 多线程安全
     * @param connection
     * @throws Exception
     */

    public static void testCount(Connection connection) throws Exception{
        Table table = connection.getTable(TableName.valueOf("counters"));
        long cc = 1;
        long result = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), cc);
        System.out.println(result);

//        new Increment();
//        table.increment()
    }
}
