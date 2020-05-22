package output_and_metrics.graphics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

    static final String DB_NAME = "Query3";

    public static void main(String[] args) {

        InfluxDBClient client = new InfluxDBClient();
        System.out.println("Creating influxDB database...");
        client.createDatabase(DB_NAME, "365d");
        System.out.println("Starting influxDB import...");
        importQuery3Result(client, DB_NAME);
        System.out.println("Closing connection...");
        client.closeConnection();
        System.out.println("Done!");
    }

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

    private static void parseAndInsert(InfluxDBClient client, String dbName, String cluster, String month, int clusterIndex) {
        List<String> countries = new ArrayList<>();
        List<Double> slopes = new ArrayList<>();
        String[] elements = cluster.split(", ");
        Pattern pattern = Pattern.compile("(.*)\\((.*)\\)");
        Matcher matcher;
        for (String element : elements) {
            matcher = pattern.matcher(element);
            if (matcher.find()) {
                countries.add(matcher.group(1));
                slopes.add(Double.parseDouble(matcher.group(2)));
            } else {
                System.err.println("Error parsing: " + element);
            }
        }
        client.insertPoints(dbName, slopes, countries, clusterIndex, month);
    }
}
