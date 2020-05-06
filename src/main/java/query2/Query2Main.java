package query2;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.GeoCoordinate;

import java.util.Arrays;
import java.util.List;

public class Query2Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataset2 = sparkContext.textFile("data/DS2.csv");

        //TODO: remove header in csv and if needed include date in the list

        // convert data in RDD
        JavaPairRDD<String, CountryData> data = dataset2.mapToPair(
                line -> {
                    String[] splitted = line.split(",");
                    String key = splitted[0].equals("") ? splitted[1] : splitted[0];
                    GeoCoordinate geoCoordinate = new GeoCoordinate(splitted[2], splitted[3]);
                    return new Tuple2<>(key, new CountryData(geoCoordinate,
                            Arrays.asList(splitted).subList(4,splitted.length)));
                });

        JavaPairRDD<String, CountryData> countriesWithCoefficient = data.mapToPair(
                tuple -> {
                    SimpleRegression regression = new SimpleRegression();
                    List<Double> values = tuple._2.getCovidConfirmedCases();
                    for (int i = 0; i < values.size(); i++) {
                        regression.addData(i, values.get(i));
                    }
                    Double coefficient = regression.getSlope();

                    return new Tuple2<>(tuple._1, new CountryData(tuple._2, coefficient));
                }
        );

        JavaPairRDD<Double, CountryData> orderedCountries = countriesWithCoefficient.mapToPair(
                tuple -> new Tuple2<>(tuple._2.getCovidTrendlineCoefficient(), new CountryData(tuple._2, tuple._1)))
                .sortByKey(false);
    }
}
