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

/**
 * Class that provides both getters for uri strings and writers to store data in hdfs
 */
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

    /**
     * Used to get first dataset HDFS path
     * @return path of the first dataset
     */
    public static String getDS1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET1_FILENAME;
    }

    /**
     * Used to get the second dataset HDFS path
     * @return path of the second dataset
     */
    public static String getDS2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET2_FILENAME;
    }

    /**
     * Used to get first query output directory HDFS path
     * @return first query output directory path
     */
    public static String getOutputPathQuery1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY1_RESULT;
    }

    /**
     * Used to get second query output directory HDFS path
     * @return second query output directory path
     */
    public static String getOutputPathQuery2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY2_RESULT;
    }

    /**
     * Used to get third query output HDFS path
     * @return third query output path
     */
    public static String getOutputPathQuery3() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY3_RESULT;
    }

    /**
     * Used to get HDFS URL for connection
     * @return HDFS URL
     */
    public static String getHdfs() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT;
    }

    /**
     * Formatted console print used for execution time results
     * @param time to print in milliseconds
     */
    public static void printTime(Long time) {
        System.out.println(ANSI_GREEN +
                "\nExecution completed in " +
                time +
                " milliseconds.\n" +
                ANSI_RESET);
    }

    /**
     * Used to format data param and write it as a text file in hdfs at the location specified by path param
     * @param path string representing the desired location where store the text file
     * @param data structured data representing the clusters' assignment for every month
     */
    public static void writeLocalStructureToHdfs(String path, List<Tuple2<String, ArrayList<ArrayList<String>>>> data) {
        Configuration configuration = new Configuration();
        StringBuilder builder = new StringBuilder();
        try {
            // connect to HDFS
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path file = new Path(path);
            // create file and overwrite if exists
            FSDataOutputStream outputStream = hdfs.create(file, true);
            // convert result to string file
            for (Tuple2<String, ArrayList<ArrayList<String>>> monthData : data) {
                builder.append("(").append(monthData._1()).append(",[");
                for (ArrayList<String> cluster : monthData._2()) {
                    builder.append(cluster.toString()).append(",");
                }
                builder.deleteCharAt(builder.length() - 1).append("])\n");
            }
            // perform write to HDFS file
            outputStream.writeChars(builder.toString());
            outputStream.close();
            hdfs.close();
        } catch (Exception e) {
            System.err.println("Could not save file to HDFS");
        }
    }

    /**
     * Used to write an rdd as a text file in hdfs at the location specified by path param
     * @param path string representing the desired location where store the text file
     * @param rdd JavaPairRDD with all the data
     */
    public static void writeRDDToHdfs(String path, JavaPairRDD rdd) {
        Configuration configuration = new Configuration();
        try {
            // connect to HDFS
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            // delete previous file version
            hdfs.delete(new Path(path), true);
            hdfs.close();
            // save new file version
            rdd.saveAsTextFile(path);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not save file to HDFS");
        }
    }
}
