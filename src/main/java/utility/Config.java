package utility;

public class Config {
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

    public static String putQuery1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY1_RESULT;
    }

    public static String putQuery2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY2_RESULT;
    }

    public static String putQuery3() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY3_RESULT;
    }

    public static void printTime(Long time) {
        System.out.println(ANSI_GREEN +
                "\nExecution completed in " +
                time +
                " milliseconds.\n" +
                ANSI_RESET);
    }
}
