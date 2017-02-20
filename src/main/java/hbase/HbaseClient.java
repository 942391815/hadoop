package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by qiaogu on 2017/2/20.
 */
public class HbaseClient {
    public static Connection getConnection(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
        configuration.set("hbase.master", "hadoop");

        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
