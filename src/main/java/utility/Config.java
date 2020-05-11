package utility;

public class Config {
    private final static String HDFS_NAMENODE_ADDRESS = "127.0.0.1";
    private final static String HDFS_NAMENODE_PORT = "9871";
    private final static String DATASET1_PATH = "/data/DS1.csv";
    private final static String DATASET2_PATH = "/data/DS2.csv";

    public static String getDS1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + DATASET1_PATH;
    }

    public static String getDS2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + DATASET2_PATH;
    }
}
