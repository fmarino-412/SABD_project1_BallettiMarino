package output_and_metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    private static final String QUERY3_CSV_FILE_PATH = "Results/query3_output.csv";

    public static void main(String[] args) {
        System.out.println("Generating csv for query 1 result...");
        outputFromQuery1Result(QUERY1_CSV_FILE_PATH);
        System.out.println("Generating csv for query 2 result...");
        outputFromQuery2Result(QUERY2_CSV_FILE_PATH);
        System.out.println("Generating csv for query 3 result...");
        outputFromQuery3Result(QUERY3_CSV_FILE_PATH);
        System.out.println("Check \"Results\" directory");
    }

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
                csv.createNewFile();
            }
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
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        Pattern pattern = Pattern.compile("\\((\\d+-\\d+-\\d+),\\((\\d+.\\d+),(\\d+.\\d+)\\)\\)");
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
                csv.createNewFile();
            }
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
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        Pattern pattern = Pattern.compile("\\((\\w+\\s-\\s\\d+-\\d+-\\d+),\\[(\\d+.\\d+\\w*\\d*)," +
                                "\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*),\\s(\\d+.\\d+\\w*\\d*)]\\)");
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
                csv.createNewFile();
            }
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
                Pattern pattern = Pattern.compile("\\((\\d+-\\d+),\\[\\[(.*)],\\[(.*)],\\[(.*)],\\[(.*)]]\\)");
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