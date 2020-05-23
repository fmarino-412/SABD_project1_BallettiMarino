package output_and_metrics.graphics;

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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InfluxDBImport {

    static final String DB_NAME = "Queries";

    public static void main(String[] args) {

        InfluxDBClient client = new InfluxDBClient();
        System.out.println("Creating influxDB database...");
        client.createDatabase(DB_NAME, "365d");
        System.out.println("Starting influxDB import for Query 1 output...");
        importQuery1Result(client, DB_NAME);
        System.out.println("Starting influxDB import for Query 2 output...");
        importQuery2Result(client, DB_NAME);
        System.out.println("Starting influxDB import for Query 3 output...");
        importQuery3Result(client, DB_NAME);
        System.out.println("Closing connection...");
        client.closeConnection();
        System.out.println("Done!");
    }

    /**
     * Loads results of the first query from the HDFS to an influxDB database.
     * @param client client for influxDB communication
     * @param dbName name of the db to which data must be added
     */
    private static void importQuery1Result(InfluxDBClient client, String dbName) {
        String line;

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
                            client.insertPoints(dbName,
                                    matcher.group(1),
                                    Double.valueOf(matcher.group(2)),
                                    Double.valueOf(matcher.group(3)));
                        }
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.err.println("Could not load query 1 result from HDFS");
        }
    }

    /**
     * Loads results of the second query from the HDFS to an influxDB database.
     * @param client client for influxDB communication
     * @param dbName name of the db to which data must be added
     */
    private static void importQuery2Result(InfluxDBClient client, String dbName) {
        String line;

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
                        Pattern pattern = Pattern.compile("\\((\\w+)\\s-\\s(\\d+-\\d+-\\d+),\\[(\\d+.\\d+\\w*\\d*)," +
                                "\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*)]\\)");
                        // splits the line in regex groups
                        Matcher matcher = pattern.matcher(line);

                        if (matcher.find()) {
                            client.insertPoints(dbName,
                                    matcher.group(2),
                                    matcher.group(1),
                                    Double.valueOf(matcher.group(3)),
                                    Double.valueOf(matcher.group(4)),
                                    Double.valueOf(matcher.group(5)),
                                    Double.valueOf(matcher.group(6)));
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
     * Loads results of the third query from the HDFS to an influxDB database.
     * @param client client for influxDB communication
     * @param dbName name of the db to which data must be added
     */
    private static void importQuery3Result(InfluxDBClient client, String dbName) {

        String line;
        String month;

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
                    month = matcher.group(1);
                    parseAndInsert(client, dbName, matcher.group(2), month, 1);
                    parseAndInsert(client, dbName, matcher.group(3), month, 2);
                    parseAndInsert(client, dbName, matcher.group(4), month, 3);
                    parseAndInsert(client, dbName, matcher.group(5), month, 4);
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

    /**
     * Scope: Query 3
     * Inserts cluster specific values in influxDB.
     * @param client client for influxDB communication
     * @param dbName name of the db to which data must be added
     * @param cluster list of elements belonging to the cluster passed as string
     * @param month month and year corresponding to the cluster composition
     * @param clusterIndex index of the current cluster (from 1 to 4)
     */
    private static void parseAndInsert(InfluxDBClient client, String dbName, String cluster, String month,
                                       int clusterIndex) {
        List<String> countries = new ArrayList<>();
        List<Double> slopes = new ArrayList<>();
        // splits single measurement points
        String[] elements = cluster.split(", ");
        // extract country and slope from every point
        Pattern pattern = Pattern.compile("(.*)\\((.*)\\)");
        Matcher matcher;
        for (String element : elements) {
            matcher = pattern.matcher(element);
            if (matcher.find()) {
                // add country name to list of countries
                countries.add(matcher.group(1));
                // add slope value to list of values
                slopes.add(Double.parseDouble(matcher.group(2)));
            } else {
                System.err.println("Error parsing: " + element);
            }
        }
        // compute centroid
        double centroid = slopes.stream().mapToDouble(a -> a).average().orElse(0.0);
        // perform insertion
        client.insertPoints(dbName, centroid, countries, clusterIndex, month);
    }
}
