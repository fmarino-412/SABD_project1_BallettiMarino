package utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;

import java.net.URI;

public class HdfsUtility {

    public static void writeLocalStructureToHdfs(String path, String data) {
        Configuration configuration = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(new URI(Config.getHdfs()), configuration);
            Path file = new Path(path);
            FSDataOutputStream outputStream = hdfs.create(file, true);
            outputStream.writeChars(data);
            outputStream.close();
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeRDDToHdfs(String path, JavaPairRDD rdd) throws Exception {
        rdd.saveAsTextFile(path);
    }
}
