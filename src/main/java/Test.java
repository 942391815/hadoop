/**
 * Created by qiaogu on 2017/1/23.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.URI;

public class Test {
    private static void uploadToHdfs() throws Exception {
        String localSrc = "d://process-info.log";
        String dst = "hdfs://192.168.106.128:9000/user/qq.txt";
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public static void main(String[] args) throws Exception {
        uploadToHdfs();
    }
}
