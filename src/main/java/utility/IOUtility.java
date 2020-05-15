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

public class IOUtility {
    private final static String HDFS_NAMENODE_ADDRESS = "127.0.0.1";
    private final static String HDFS_NAMENODE_PORT = "9871";
    private final static String INPUT_FOLDER = "/data";
    private final static String OUTPUT_FOLDER = "/output";
    private final static String DATASET1_FILENAME = "/DS1.csv";
    private final static String DATASET2_FILENAME = "/DS2.csv";
    private final static String QUERY1_RESULT = "/query1";
    private final static String QUERY2_RESULT = "/query2";
    private final static String QUERY3_RESULT = "/query3";

    // console colors
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";


    public static String getDS1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET1_FILENAME;
    }

    public static String getDS2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET2_FILENAME;
    }

    public static String getOutputPathQuery1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY1_RESULT;
    }

    public static String getOutputPathQuery2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY2_RESULT;
    }

    public static String getOutputPathQuery3() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY3_RESULT;
    }

    public static String getHdfs() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT;
    }

    public static void printTime(Long time) {
        System.out.println(ANSI_GREEN +
                "\nExecution completed in " +
                time +
                " milliseconds.\n" +
                ANSI_RESET);
    }

    public static void writeLocalStructureToHdfs(String path, List<Tuple2<String, ArrayList<ArrayList<String>>>> data) {
        Configuration configuration = new Configuration();
        StringBuilder builder = new StringBuilder();
        try {
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
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
