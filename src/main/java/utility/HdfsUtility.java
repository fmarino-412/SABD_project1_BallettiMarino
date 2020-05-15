package utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtility {

    public static void writeLocalStructureToHdfs(String path, List<Tuple2<String, ArrayList<ArrayList<String>>>> data) {
        Configuration configuration = new Configuration();
        StringBuilder builder = new StringBuilder();
        try {
            FileSystem hdfs = FileSystem.get(new URI(Config.getHdfs()), configuration);
            Path file = new Path(path);
            FSDataOutputStream outputStream = hdfs.create(file, true);
            // convert result to string file
            for (Tuple2<String, ArrayList<ArrayList<String>>> monthData : data) {
                builder.append("(").append(monthData._1()).append(",[");
                for (ArrayList<String> cluster : monthData._2()) {
                    builder.append(cluster.toString()).append(",");
                }
                builder.deleteCharAt(builder.length() - 1).append("])\n");
            }
            outputStream.writeChars(builder.toString());
            outputStream.close();
            hdfs.close();
        } catch (Exception e) {
            System.err.println("Could not save file to HDFS");
        }
    }

    public static void writeRDDToHdfs(String path, JavaPairRDD rdd) {
        try {
            rdd.saveAsTextFile(path);
        } catch (Exception e) {
            System.err.println("This query output has already been saved in HDFS");
        }
    }
}
