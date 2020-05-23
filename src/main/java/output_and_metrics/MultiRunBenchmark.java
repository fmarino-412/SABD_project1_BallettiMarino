package output_and_metrics;

import query1.Query1Main;
import query1.Query1SparkSQL;
import query2.Query2Main;
import query2.Query2SparkSQL;
import query3.Query3Main;
import utility.ClusteringUtility;

/**
 * Class used to perform a multi run for every query execution.
 * Time results of this multi run were used to realize the benchmark file
 */

public class MultiRunBenchmark {

    // TODO: confrontare costi MLLib con costi Naive

    private static final int RUNS = 100;

    public static void main(String[] args) {

        System.out.println("Query 1 multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query1Main.main(null);
        }

        System.out.println("Query 2 multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query2Main.main(null);
        }

        System.out.println("Query 3 Naive multi-run");
        ClusteringUtility.setNaive(true);
        for (int i = 0; i < RUNS; i++) {
            Query3Main.main(null);
        }

        System.out.println("Query 3 MLlib multi-run");
        ClusteringUtility.setNaive(false);
        for (int i = 0; i < RUNS; i++) {
            Query3Main.main(null);
        }

        System.out.println("Query 1 SparkSQL multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query1SparkSQL.main(null);
        }

        System.out.println("Query 2 SparkSQL multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query2SparkSQL.main(null);
        }
    }
}
