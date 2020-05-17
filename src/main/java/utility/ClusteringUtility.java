package utility;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import query3.CountryDataQuery3;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ClusteringUtility {

    // choose to perform clustering using mllib or loyd naive implementation
    private static final boolean NAIVE = false;

    private static final Integer CLUSTERS = 4;
    private static final Integer ITERATION = 20;
    private static final String INITIALIZATION_MODE = "random";
    private static final Integer SEED = 123456789;

    private static JavaPairRDD<Double, String> prepareData(JavaPairRDD<String, List<Tuple2<Double,
            CountryDataQuery3>>> data) {
        return data.flatMapToPair(
                tuple -> {
                    ArrayList<Tuple2<Double, String>> result = new ArrayList<>();
                    for (Tuple2<Double, CountryDataQuery3> elem : tuple._2()) {
                        result.add(new Tuple2<>(elem._1(), elem._2().getName()));
                    }
                    return result.iterator();
                }
        );
    }

    private static ArrayList<ArrayList<String>> clusteringNaive(JavaPairRDD<String, List<Tuple2<Double,
            CountryDataQuery3>>> data) {

        Random random = new Random(SEED);

        List<Double> centroids = new ArrayList<>();

        // centroids random initialization
        for (int i = 0; i < CLUSTERS; i++) {
            centroids.add(random.nextDouble());
        }

        JavaPairRDD<Double, String> toCluster = prepareData(data).cache();

        // evaluate centroids
        for (int i = 0; i < ITERATION; i++) {
            List<Tuple2<Integer, Double>> newCentroids = toCluster.mapToPair(
                    tuple -> new Tuple2<>(assignToCluster(tuple._1(), centroids), tuple._1())
            ).groupByKey().mapToPair(
                    tuple -> {
                        int numOfPoints = 0;
                        double sumOfPoints = 0.0;
                        for (double point : tuple._2()) {
                            numOfPoints++;
                            sumOfPoints += point;
                        }
                        return new Tuple2<>(tuple._1(), sumOfPoints/numOfPoints);
                    }
            ).collect();

            for (Tuple2<Integer, Double> newCentroid : newCentroids) {
                centroids.set(newCentroid._1(), newCentroid._2());
            }
        }

        // initialization of cost computing structure
        ArrayList<ArrayList<Double>> costData = new ArrayList<>();
        // initialization of result structure
        ArrayList<ArrayList<String>> result = new ArrayList<>();

        for (int i = 0; i < CLUSTERS; i++) {
            costData.add(new ArrayList<>());
            result.add(new ArrayList<>());
        }

        int clusterAssignment;
        List<Tuple2<Double, String>> toPredict = toCluster.collect();
        for (Tuple2<Double, String> elem : toPredict) {
            clusterAssignment = assignToCluster(elem._1(), centroids);
            result.get(clusterAssignment).add(elem._2());
            costData.get(clusterAssignment).add(elem._1());
        }

        // compute cost as sum of squared intra cluster distances
        double totalCost = 0;
        for (int i = 0; i < CLUSTERS; i++) {
            for (Double singlePoint : costData.get(i)) {
                totalCost += Math.pow(euclideanDistance(singlePoint, centroids.get(i)), 2);
            }
        }
        System.out.println("Total cost: " + totalCost);

        return result;
    }

    private static double euclideanDistance(double val1, double val2) {
        return Math.abs(val1 - val2);
    }

    private static int assignToCluster(double point, List<Double> centroids) {
        int indexOfCetroid = -1;
        double minDistance = Double.MAX_VALUE;
        double currentDistance;

        for (Double centroid : centroids) {
            currentDistance = euclideanDistance(centroid, point);
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                indexOfCetroid = centroids.indexOf(centroid);
            }
        }

        return indexOfCetroid;
    }

    private static ArrayList<ArrayList<String>> clusteringMLlib(JavaPairRDD<String, List<Tuple2<Double,
            CountryDataQuery3>>> data) {

        JavaPairRDD<Double, String> toCluster = prepareData(data).cache();

        JavaRDD<Vector> values = toCluster.map(
                tuple -> Vectors.dense(tuple._1())
        );

        KMeansModel model = KMeans.train(values.rdd(), CLUSTERS, ITERATION, INITIALIZATION_MODE, SEED);
        System.out.println("Total cost: " + model.computeCost(values.rdd()));
        List<Tuple2<Double, String>> toPredict = toCluster.collect();

        // initialization of result structure
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        for (int i = 0; i < CLUSTERS; i++) {
            result.add(new ArrayList<>());
        }

        for (Tuple2<Double, String> elem : toPredict) {
            int clusterIndex = model.predict(Vectors.dense(elem._1()));
            result.get(clusterIndex).add(elem._2());
        }

        return result;
    }

    public static ArrayList<ArrayList<String>> performClustering(JavaPairRDD<String,
            List<Tuple2<Double, CountryDataQuery3>>> data) {

        if (NAIVE) {
            return clusteringNaive(data);
        } else {
            return clusteringMLlib(data);
        }
    }
}
