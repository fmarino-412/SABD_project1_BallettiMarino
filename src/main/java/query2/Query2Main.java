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
        JavaPairRDD<Double, CountryData> data = dataset2.mapToPair(
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

        JavaRDD<Tuple2<Double, CountryData>> topCountries = data.sortByKey(false).zipWithIndex()
                .filter(xi -> xi._2 < 100).keys();
    }
}
