package query2;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import utility.GeoCoordinate;

import java.util.Arrays;
import java.util.List;

public class Query2Preprocessing {

    public static JavaRDD<Tuple2<Double, CountryDataQuery2>> preprocessData(JavaRDD<String> dataset2) {
        // convert data in RDD
         return dataset2.mapToPair(
                line -> {
                    String[] splitted = line.split(",");
                    GeoCoordinate geoCoordinate = new GeoCoordinate(splitted[2], splitted[3]);
                    CountryDataQuery2 countryData = new CountryDataQuery2(geoCoordinate,
                            Arrays.asList(splitted).subList(4,splitted.length));

                    SimpleRegression regression = new SimpleRegression();
                    List<Double> values = countryData.getCovidConfirmedCases();
                    for (int i = 0; i < values.size(); i++) {
                        regression.addData(i, values.get(i));
                    }
                    return new Tuple2<>(regression.getSlope(), countryData);

                // sort by slope in descending order and take out just the first 100 elements
                }).sortByKey(false).zipWithIndex().filter(xi -> xi._2() < 100).keys();
    }
}
