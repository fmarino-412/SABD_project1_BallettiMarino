package output_and_metrics.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utility.IOUtility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used to export spark computation results from HDFS to an HBASE datastore.
 * In order to use this class make sure to have in /etc/hosts the line "127.0.0.1  hbase".
 */

public class HBaseImport {

    private static final String TABLE_QUERY1 = "Query1_hbase_table";
    private static final String TABLE_QUERY2 = "Query2_hbase_table";
    private static final String TABLE_QUERY3 = "Query3_hbase_table";

    // Query 1 table structure
    private static final String TABLE_QUERY1_CF = "Statistics";
    private static final String TABLE_QUERY1_C1 = "Cured";
    private static final String TABLE_QUERY1_C2 = "Swabs";

    // Query 2 table structure
    private static final String TABLE_QUERY2_CF = "Statistics";
    private static final String TABLE_QUERY2_C1 = "Mean";
    private static final String TABLE_QUERY2_C2 = "Standard_Deviation";
    private static final String TABLE_QUERY2_C3 = "Minimum";
    private static final String TABLE_QUERY2_C4 = "Maximum";

    // Query 3 table structure
    private static final String TABLE_QUERY3_CF = "Clusters";
    private static final String TABLE_QUERY3_C1 = "Cluster_1";
    private static final String TABLE_QUERY3_C2 = "Cluster_2";
    private static final String TABLE_QUERY3_C3 = "Cluster_3";
    private static final String TABLE_QUERY3_C4 = "Cluster_4";


    public static void main(String[] args) {

        HBaseLightClient client = new HBaseLightClient();

        System.out.println("Htable started!");
        System.out.println("Preparing environment...");
        if (client.exists(TABLE_QUERY1)) {
            client.deleteTable(TABLE_QUERY1);
        }
        if (client.exists(TABLE_QUERY2)) {
            client.deleteTable(TABLE_QUERY2);
        }
        if (client.exists(TABLE_QUERY3)) {
            client.deleteTable(TABLE_QUERY3);
        }

        System.out.println("Htable environment ready!");
        System.out.println("Creating tables...");
        client.createTable(TABLE_QUERY1, TABLE_QUERY1_CF);
        client.createTable(TABLE_QUERY2, TABLE_QUERY2_CF);
        client.createTable(TABLE_QUERY3, TABLE_QUERY3_CF);

        System.out.println("Importing hdfs data to tables...");
        importQuery1Result(client);
        importQuery2Result(client);
        importQuery3Result(client);

        System.out.println("-----------------------\nPrinting Query 1 table:");
        client.printTable(TABLE_QUERY1);

        System.out.println("-----------------------\nPrinting Query 2 table:");
        client.printTable(TABLE_QUERY2);

        System.out.println("-----------------------\nPrinting Query 3 table:");
        client.printTable(TABLE_QUERY3);

        client.closeConnection();
    }

    /**
     * Loads results of the first query from the HDFS to an hbase table
     * @param hBaseLightClient client for htable communication
     */
    private static void importQuery1Result(HBaseLightClient hBaseLightClient) {

        String line;
        String key;
        String cured;
        String swabs;

        Configuration configuration = new Configuration();

        try {
            FSDataInputStream inputStream;
            BufferedReader br;
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path dirPath = new Path(IOUtility.getOutputPathQuery1());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);
            // in case of splitted file output
            for (FileStatus fileStatus : fileStatuses) {
                // _SUCCESS file and subdirectories are ignored
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        // regex describing every line structure in the query 1 result file
                        Pattern pattern = Pattern.compile("\\((\\d+-\\d+-\\d+),\\((\\d+.\\d+),(\\d+.\\d+)\\)\\)");
                        // splits the line in regex groups
                        Matcher matcher = pattern.matcher(line);

                        if (matcher.find()) {
                            key = matcher.group(1);
                            cured = matcher.group(2);
                            swabs = matcher.group(3);
                            hBaseLightClient.put(TABLE_QUERY1, key,
                                    TABLE_QUERY1_CF, TABLE_QUERY1_C1, cured,
                                    TABLE_QUERY1_CF, TABLE_QUERY1_C2, swabs);
                        }
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 1 result from HDFS");
        }
    }

    /**
     * Loads results of the second query from the HDFS to an hbase table
     * @param hBaseLightClient client for htable communication
     */
    private static void importQuery2Result(HBaseLightClient hBaseLightClient) {

        String line;
        String key;
        String mean;
        String max;
        String min;
        String stdDev;

        Configuration configuration = new Configuration();

        try {
            FSDataInputStream inputStream;
            BufferedReader br;
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path dirPath = new Path(IOUtility.getOutputPathQuery2());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);
            // in case of splitted file output
            for (FileStatus fileStatus : fileStatuses) {
                // _SUCCESS file and subdirectories are ignored
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        // regex describing every line structure in the query 2 result file
                        Pattern pattern = Pattern.compile("\\((\\w+\\s-\\s\\d+-\\d+-\\d+),\\[(\\d+.\\d+\\w*\\d*)," +
                                "\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*)]\\)");
                        // splits the line in regex groups
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
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 2 result from HDFS");
        }
    }

    /**
     * Loads results of the third query from the HDFS to an hbase table
     * @param hBaseLightClient client for htable communication
     */
    private static void importQuery3Result(HBaseLightClient hBaseLightClient) {

        String line;
        String cluster1;
        String cluster2;
        String cluster3;
        String cluster4;
        String key;

        Configuration configuration = new Configuration();

        try {
            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            // unique file, no need to iterate on folder
            Path file = new Path(IOUtility.getOutputPathQuery3());
            FSDataInputStream inputStream = hdfs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_16));

            while ((line = br.readLine()) != null) {
                // regex describing every line structure in the query 3 result file
                Pattern pattern = Pattern.compile("\\((\\d+-\\d+),\\[\\[(.*)],\\[(.*)],\\[(.*)],\\[(.*)]]\\)");
                // splits the line in regex groups
                Matcher matcher = pattern.matcher(line);

                if (matcher.find()) {
                    key = matcher.group(1);
                    cluster1 = matcher.group(2);
                    cluster2 = matcher.group(3);
                    cluster3 = matcher.group(4);
                    cluster4 = matcher.group(5);
                    hBaseLightClient.put(TABLE_QUERY3, key,
                            TABLE_QUERY3_CF, TABLE_QUERY3_C1, cluster1,
                            TABLE_QUERY3_CF, TABLE_QUERY3_C2, cluster2,
                            TABLE_QUERY3_CF, TABLE_QUERY3_C3, cluster3,
                            TABLE_QUERY3_CF, TABLE_QUERY3_C4, cluster4);
                }
            }

            br.close();
            inputStream.close();
            hdfs.close();
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
            System.err.println("Could not load query 3 result from HDFS");
        }
    }
}
