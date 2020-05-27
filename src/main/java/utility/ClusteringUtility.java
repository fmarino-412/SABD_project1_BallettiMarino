package utility;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import query3.CountryDataQuery3;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Class that provides functions to perform clustering on query 3 data
 */
public class ClusteringUtility {

    // choose to perform clustering using mllib or lloyd naive implementation
    private static boolean NAIVE = false;

    private static final Integer CLUSTERS = 4;
    private static final Integer ITERATION = 20;
    private static final String INITIALIZATION_MODE = "random";
    private static final Integer SEED = 123456789;

    /**
     * Naive scope.
     * Naive implementation of the k-means clustering based on the Lloyd's algorithm
     * @param data JavaPairRDD containing the points on which the algorithm will be performed
     * @return the clustering result
     */
    private static ArrayList<ArrayList<String>> clusteringNaive(JavaPairRDD<String, List<Tuple2<Double,
            CountryDataQuery3>>> data) {

        Random random = new Random(SEED);

        List<Double> centroids = new ArrayList<>();

        // centroids random initialization
        for (int i = 0; i < CLUSTERS; i++) {
            centroids.add(random.nextDouble());
        }

        JavaPairRDD<Double, String> toCluster = prepareData(data).cache();

        for (int i = 0; i < ITERATION; i++) {
            // perform cluster assignment
            List<Tuple2<Integer, Double>> newCentroids = toCluster.mapToPair(
                    tuple -> new Tuple2<>(assignToCluster(tuple._1(), centroids), tuple._1())
            ).groupByKey().mapToPair(
                    // evaluate new centroids
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

            // update current centroid versions
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
        DecimalFormat doubleFormat = new DecimalFormat("#.###");
        for (Tuple2<Double, String> elem : toPredict) {
            clusterAssignment = assignToCluster(elem._1(), centroids);
            result.get(clusterAssignment).add(elem._2()+"("+doubleFormat.format(elem._1())+")");
            costData.get(clusterAssignment).add(elem._1());
        }

        //computeCost(costData, centroids);

        return result;
    }

    /**
     * Naive scope.
     * Evaluate the euclidean distance between two values
     * @param val1 double
     * @param val2 double
     * @return absolute value of the difference between the points
     */
    private static double euclideanDistance(double val1, double val2) {
        return Math.abs(val1 - val2);
    }

    /**
     * Naive scope.
     * Choose the nearest centroid from a point
     * @param point to assign
     * @param centroids list of centroids
     * @return the index of the nearest centroid
     */
    private static int assignToCluster(double point, List<Double> centroids) {
        int indexOfCetroid = -1;
        double minDistance = Double.MAX_VALUE;
        double currentDistance;

        for (Double centroid : centroids) {
            // check the euclidean distance between centroid and point to find the minimum
            currentDistance = euclideanDistance(centroid, point);
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                // keep track of the centroid's index to be returned
                indexOfCetroid = centroids.indexOf(centroid);
            }
        }

        return indexOfCetroid;
    }

    /**
     * Naive scope.
     * Print the sum of squared intra cluster distances that represents the "cost" of the solution
     * @param costData arrays of points, one array for cluster
     * @param centroids list of centroids
     */
    private static void computeCost(ArrayList<ArrayList<Double>> costData, List<Double> centroids) {
        // compute cost as sum of squared intra cluster distances
        double totalCost = 0;
        for (int i = 0; i < CLUSTERS; i++) {
            for (Double singlePoint : costData.get(i)) {
                totalCost += Math.pow(euclideanDistance(singlePoint, centroids.get(i)), 2);
            }
        }
        System.out.println("Total cost: " + totalCost);
    }

    /**
     * MLLib scope.
     * Performs the clustering using MLlib
     * @param data JavaPairRDD containing the points on which the algorithm will be performed
     * @return the clustering result
     */
    private static ArrayList<ArrayList<String>> clusteringMLlib(JavaPairRDD<String, List<Tuple2<Double,
            CountryDataQuery3>>> data) {

        JavaPairRDD<Double, String> toCluster = prepareData(data).cache();

        JavaRDD<Vector> values = toCluster.map(
                tuple -> Vectors.dense(tuple._1())
        );

        // model initialization
        KMeansModel model = KMeans.train(values.rdd(), CLUSTERS, ITERATION, INITIALIZATION_MODE, SEED);
        //System.out.println("Total cost: " + model.computeCost(values.rdd()));
        List<Tuple2<Double, String>> toPredict = toCluster.collect();

        // initialization of result structure
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        for (int i = 0; i < CLUSTERS; i++) {
            result.add(new ArrayList<>());
        }

        // fill result structure with model predictions
        DecimalFormat doubleFormat = new DecimalFormat("#.###");
        for (Tuple2<Double, String> elem : toPredict) {
            int clusterIndex = model.predict(Vectors.dense(elem._1()));
            result.get(clusterIndex).add(elem._2()+"("+doubleFormat.format(elem._1())+")");
        }

        return result;
    }

    /**
     * Global scope.
     * Used to transform monthly data (for every single month separately) from the format [Month, List of countries
     * and values] to the format [value, country] in order to apply the k-means algorithm
     */
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

    /**
     * Global scope.
     * Wrapper for the clustering function, if NAIVE has been set to true it calls the naive function else it calls
     * the MLlib function
     */
    public static ArrayList<ArrayList<String>> performClustering(JavaPairRDD<String,
            List<Tuple2<Double, CountryDataQuery3>>> data) {
        if (NAIVE) {
            return clusteringNaive(data);
        } else {
            return clusteringMLlib(data);
        }
    }

    /**
     * Global scope.
     * To set programmatically the clustering mode
     * @param naiveMode true to run naive clustering algorithm, false to run mllib clustering algorithm
     */
    public static void setNaive(boolean naiveMode) {
        NAIVE = naiveMode;
    }
}
