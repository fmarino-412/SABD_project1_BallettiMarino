package output_and_metrics;

import query1.Query1Main;
import query2.Query2Main;
import query3.Query3Main;

/**
 * Class used to perform a multi run for every query execution.
 * Time results of this multi run were used to realize the benchmark file
 */

public class MultiRunBenchmark {

    private static int RUNS = 100;

    public static void main(String[] args) {

        System.out.println("Query 1 multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query1Main.main(null);
        }

        System.out.println("Query 2 multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query2Main.main(null);
        }

        System.out.println("Query 3 multi-run");
        for (int i = 0; i < RUNS; i++) {
            Query3Main.main(null);
        }
    }
}
