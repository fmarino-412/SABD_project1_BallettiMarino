package query2;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.ContinentDecoder;
import utility.GeoCoordinate;

import java.util.*;

public class Query2Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataset2 = sparkContext.textFile("data/DS2.csv");

        // convert data in RDD
        JavaPairRDD<Double, CountryData> data = dataset2.filter(
                line -> !line.startsWith("Province")
        ).mapToPair(
                line -> {
                    String[] splitted = line.split(",");
                    String key = splitted[0].equals("") ? splitted[1] : splitted[0];
                    GeoCoordinate geoCoordinate = new GeoCoordinate(splitted[2], splitted[3]);
                    CountryData countryData = new CountryData(geoCoordinate,
                            Arrays.asList(splitted).subList(4,splitted.length), key);

                    SimpleRegression regression = new SimpleRegression();
                    List<Double> values = countryData.getCovidConfirmedCases();
                    for (int i = 0; i < values.size(); i++) {
                        regression.addData(i, values.get(i));
                    }
                    return new Tuple2<>(regression.getSlope(), countryData);
                });

        JavaPairRDD<String, List<Double>> continents = data.sortByKey(false).zipWithIndex()
                .filter(xi -> xi._2 < 100).keys().mapToPair(
                        tuple -> new Tuple2<>(ContinentDecoder.detectContinent(tuple._2().getCoordinate()),
                                tuple._2().getCovidConfirmedCases())
                ).reduceByKey(
                        (x, y) -> {
                            List<Double> sum = new ArrayList<>();
                            for (int i = 0; i < x.size(); i++) {
                                sum.add(x.get(i) + y.get(i));
                            }
                            return sum;
                        }
                );

        JavaPairRDD<String, Map<String, List<Double>>> statistics = continents.mapToPair(
                        tuple -> {
                            List<Double> weeklyMeans = new ArrayList<>();
                            List<Double> weeklyStdDev = new ArrayList<>();
                            List<Double> weeklyMin = new ArrayList<>();
                            List<Double> weeklyMax = new ArrayList<>();

                            List<Double> weekValues = new ArrayList<>();

                            for (int i = 0; i < tuple._2().size(); i++) {
                                weekValues.add(tuple._2().get(i));

                                // if it was the last day of the week
                                if (i%7 == 6 || i == tuple._2().size() - 1) {

                                    // compute weekly min and max
                                    weeklyMin.add(Collections.min(weekValues));
                                    weeklyMax.add(Collections.max(weekValues));

                                    // compute weekly mean values
                                    double mean = 0.0;
                                    for (Double value : weekValues) {
                                        mean += value;
                                    }
                                    mean = mean / weekValues.size();
                                    weeklyMeans.add(mean);

                                    // compute standard deviation
                                    double stddev = 0.0;
                                    for (Double value : weekValues) {
                                        stddev = stddev + Math.pow((value - mean), 2);
                                    }
                                    stddev = Math.sqrt(stddev / weekValues.size());
                                    weeklyStdDev.add(stddev);

                                    // reset temporary list
                                    weekValues.clear();
                                }
                            }

                            Map<String, List<Double>> weeklyStatistics = new HashMap<>();
                            weeklyStatistics.put("Weekly Means", weeklyMeans);
                            weeklyStatistics.put("Weekly Standard Deviations", weeklyStdDev);
                            weeklyStatistics.put("Weekly Mins", weeklyMin);
                            weeklyStatistics.put("Weekly Maxs", weeklyMax);

                            return new Tuple2<>(tuple._1(), weeklyStatistics);
                        }
                );

        Map<String, Map<String, List<Double>>> results = statistics.collectAsMap();
        System.out.println(results);
    }
}
