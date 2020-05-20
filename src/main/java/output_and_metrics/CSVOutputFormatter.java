package output_and_metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import query1.Query1Main;
import query2.Query2Main;
import query3.Query3Main;
import utility.ClusteringUtility;
import utility.IOUtility;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used to export spark computation results from HDFS to CSV file in the Result directory
 */

public class CSVOutputFormatter {

    private static final String QUERY1_CSV_FILE_PATH = "Results/query1_output.csv";
    private static final String QUERY2_CSV_FILE_PATH = "Results/query2_output.csv";
    private static final String QUERY3_MLLIB_CSV_FILE_PATH = "Results/query3_mllib_output.csv";
    private static final String QUERY3_NAIVE_CSV_FILE_PATH = "Results/query3_naive_output.csv";

    public static void main(String[] args) {
        System.out.println("Executing query 1...");
        Query1Main.main(null);
        System.out.println("Generating csv for query 1 result...");
        outputFromQuery1Result(QUERY1_CSV_FILE_PATH);
        System.out.println("\n\nExecuting query 2...");
        Query2Main.main(null);
        System.out.println("Generating csv for query 2 result...");
        outputFromQuery2Result(QUERY2_CSV_FILE_PATH);
        System.out.println("\n\nExecuting query 3 with naive clustering...");
        ClusteringUtility.setNaive(true);
        Query3Main.main(null);
        System.out.println("Generating csv for query 3 result with naive clustering...");
        outputFromQuery3Result(QUERY3_NAIVE_CSV_FILE_PATH);
        System.out.println("\n\nExecuting query 3 with mllib clustering...");
        ClusteringUtility.setNaive(false);
        Query3Main.main(null);
        System.out.println("Generating csv for query 3 result with mllib clustering...");
        outputFromQuery3Result(QUERY3_MLLIB_CSV_FILE_PATH);
        System.out.println("\n\n\nCheck \"Results\" directory");
    }

    /**
     * Exports results of the first query from the HDFS to a file
     * @param csvPath path of the destination file
     */
    private static void outputFromQuery1Result(String csvPath) {

        String line;

        Configuration configuration = new Configuration();

        try {
            // input structures
            FSDataInputStream inputStream;
            BufferedReader br;

            // output structures
            File csv = new File(csvPath);
            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            BufferedWriter bw = new BufferedWriter(writer);
            StringBuilder builder = new StringBuilder();

            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path dirPath = new Path(IOUtility.getOutputPathQuery1());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);

            // header
            builder.delete(0, builder.length());
            builder.append("Week start date")
                    .append(";")
                    .append("Cured mean number")
                    .append(";")
                    .append("Swabs mean number")
                    .append("\n");
            bw.append(builder.toString());

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
                            builder.delete(0, builder.length());
                            builder.append(matcher.group(1))
                                    .append(";")
                                    .append(matcher.group(2))
                                    .append(";")
                                    .append(matcher.group(3))
                                    .append("\n");
                            bw.append(builder.toString());
                        }
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
            bw.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 1 result from HDFS to CSV file");
        }
    }

    /**
     * Exports results of the second query from the HDFS to a file
     * @param csvPath path of the destination file
     */
    private static void outputFromQuery2Result(String csvPath) {

        String line;
        String[] key;

        Configuration configuration = new Configuration();

        try {
            // input structures
            FSDataInputStream inputStream;
            BufferedReader br;

            // output structures
            File csv = new File(csvPath);
            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            BufferedWriter bw = new BufferedWriter(writer);
            StringBuilder builder = new StringBuilder();

            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            Path dirPath = new Path(IOUtility.getOutputPathQuery2());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);

            // header
            builder.delete(0, builder.length());
            builder.append("Continent")
                    .append(";")
                    .append("Week start date")
                    .append(";")
                    .append("Mean of positive cases")
                    .append(";")
                    .append("Standard deviation of positive cases")
                    .append(";")
                    .append("Minimum of positive cases")
                    .append(";")
                    .append("Maximum of positive cases")
                    .append("\n");
            bw.append(builder.toString());

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
                            builder.delete(0, builder.length());
                            key = matcher.group(1).split(" - ");
                            builder.append(key[0])
                                    .append(";")
                                    .append(key[1])
                                    .append(";")
                                    .append(matcher.group(2))
                                    .append(";")
                                    .append(matcher.group(3))
                                    .append(";")
                                    .append(matcher.group(4))
                                    .append(";")
                                    .append(matcher.group(5))
                                    .append("\n");
                            bw.append(builder.toString());
                        }
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
            bw.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 2 result from HDFS to CSV file");
        }
    }

    /**
     * Exports results of the third query from the HDFS to a file
     * @param csvPath path of the destination file
     */
    private static void outputFromQuery3Result(String csvPath) {

        String line;

        Configuration configuration = new Configuration();

        try {
            // input structures
            FSDataInputStream inputStream;
            BufferedReader br;

            // output structures
            File csv = new File(csvPath);
            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            BufferedWriter bw = new BufferedWriter(writer);
            StringBuilder builder = new StringBuilder();

            // header
            builder.delete(0, builder.length());
            builder.append("Month and year")
                    .append(";")
                    .append("First cluster")
                    .append(";")
                    .append("Second cluster")
                    .append(";")
                    .append("Third cluster")
                    .append(";")
                    .append("Fourth cluster")
                    .append("\n");
            bw.append(builder.toString());

            FileSystem hdfs = FileSystem.get(new URI(IOUtility.getHdfs()), configuration);
            // unique file, no need to iterate on folder
            Path file = new Path(IOUtility.getOutputPathQuery3());
            inputStream = hdfs.open(file);
            br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_16));

            while ((line = br.readLine()) != null) {
                // regex describing every line structure in the query 3 result file
                Pattern pattern = Pattern.compile("\\((\\d+-\\d+),\\[\\[(.*)],\\[(.*)],\\[(.*)],\\[(.*)]]\\)");
                // splits the line in regex groups
                Matcher matcher = pattern.matcher(line);

                if (matcher.find()) {
                    builder.delete(0, builder.length());
                    builder.append(matcher.group(1))
                            .append(";")
                            .append("[")
                            .append(matcher.group(2))
                            .append("]")
                            .append(";")
                            .append("[")
                            .append(matcher.group(3))
                            .append("]")
                            .append(";")
                            .append("[")
                            .append(matcher.group(4))
                            .append("]")
                            .append(";")
                            .append("[")
                            .append(matcher.group(5))
                            .append("]")
                            .append("\n");
                    bw.append(builder.toString());
                }
            }
            br.close();
            inputStream.close();
            hdfs.close();
            bw.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 3 result from HDFS to CSV file");
        }
    }
}
