package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.util.matching.Regex;
import utility.IOUtility;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HbaseImport {

    private static final String TABLE_QUERY1 = "Weekly Mean Swabs and Cured in Italy";
    private static final String TABLE_QUERY2 = "Continent Weekly Statistics";
    private static final String TABLE_QUERY3 = "Monthly Countries Clustering";

    // Query 1 table structure
    private static final String TABLE_QUERY1_CF = "Statistics";
    private static final String TABLE_QUERY1_C1 = "Cured";
    private static final String TABLE_QUERY1_C2 = "Swabs";

    // Query 2 table structure
    private static final String TABLE_QUERY2_CF = "Statistics";
    private static final String TABLE_QUERY2_C1 = "Mean";
    private static final String TABLE_QUERY2_C2 = "Standard Deviation";
    private static final String TABLE_QUERY2_C3 = "Minimum";
    private static final String TABLE_QUERY2_C4 = "Maximum";

    // Query 3 table structure
    private static final String TABLE_QUERY3_CF = "Clusters";
    private static final String TABLE_QUERY3_C1 = "Cluster 1";
    private static final String TABLE_QUERY3_C2 = "Cluster 2";
    private static final String TABLE_QUERY3_C3 = "Cluster 3";
    private static final String TABLE_QUERY3_C4 = "Cluster 4";

    public static void main(String[] args) {

        HBaseLightClient client = new HBaseLightClient();

        if (client.exists(TABLE_QUERY1)) {
            client.deleteTable(TABLE_QUERY1);
        }
        if (client.exists(TABLE_QUERY2)) {
            client.deleteTable(TABLE_QUERY2);
        }
        if (client.exists(TABLE_QUERY3)) {
            client.deleteTable(TABLE_QUERY3);
        }

        client.createTable(TABLE_QUERY1, TABLE_QUERY1_CF);
        client.createTable(TABLE_QUERY2, TABLE_QUERY2_CF);
        client.createTable(TABLE_QUERY3, TABLE_QUERY3_CF);

        importQuery1Result(client);
        importQuery2Result(client);
        importQuery3Result(client);
    }

    private static String importQuery1Result(HBaseLightClient hBaseLightClient) {
        return null;
    }

    private static void importQuery2Result(HBaseLightClient hBaseLightClient) {

        String line;
        String key;
        String mean;
        String max;
        String min;
        String stdDev;

        Configuration configuration = new Configuration();

        try {
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path file = new Path(IOUtility.getOutputPathQuery2());
            FSDataInputStream inputStream = hdfs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            while ((line = br.readLine()) != null) {

                Pattern pattern = Pattern.compile("\\((\\w+\\s-\\s\\d+-\\d+-\\d+),\\[(\\d+.\\d+),\\s(\\d+.\\d+)" +
                        ",\\s(\\d+.\\d+),\\s(\\d+.\\d+)]\\)");
                Matcher matcher = pattern.matcher(line);

                if (matcher.find()) {
                    key = matcher.group(1);
                    mean = matcher.group(2);
                    stdDev = matcher.group(3);
                    min = matcher.group(4);
                    max = matcher.group(5);
                    hBaseLightClient.put(TABLE_QUERY2, key,
                            TABLE_QUERY2_CF, TABLE_QUERY2_C1, mean,
                            TABLE_QUERY2_CF, TABLE_QUERY2_C2, stdDev,
                            TABLE_QUERY2_CF, TABLE_QUERY2_C3, min,
                            TABLE_QUERY2_CF, TABLE_QUERY2_C4, max);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 2 result from HDFS");
        }
    }

    private static String importQuery3Result(HBaseLightClient hBaseLightClient) {
        return null;
    }
}
