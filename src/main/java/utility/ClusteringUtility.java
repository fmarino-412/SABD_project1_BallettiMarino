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

public class ClusteringUtility {

    private static final boolean NAIVE = Boolean.FALSE;
    private static final Integer CLUSTERS = 4;
    private static final Integer ITERATION = 20;

    public void clusteringNaive() {

    }

    public static void clusteringMLlib(JavaPairRDD<String, List<Tuple2<Double, CountryDataQuery3>>> data, String month) {

        JavaPairRDD<Double, String> toCluster = data.flatMapToPair(
                tuple -> {
                    ArrayList<Tuple2<Double, String>> result = new ArrayList<>();
                    for (Tuple2<Double, CountryDataQuery3> elem : tuple._2()) {
                        result.add(new Tuple2<>(elem._1(), elem._2().getName()));
                    }
                    return result.iterator();
                }
        );

        JavaRDD<Vector> values = toCluster.map(
                tuple -> Vectors.dense(tuple._1())
        );

        KMeansModel model = KMeans.train(values.rdd(), CLUSTERS, ITERATION);
        //model.predict(values.rdd()).collect();

        System.out.println(month + " results:");
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("Cluster centers:");
        for (Vector center: model.clusterCenters()) {
            System.out.println(" " + center);
        }
        System.out.println("-----------------------------------------------------------------------------------------");
    }
}
