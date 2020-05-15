package hbase;

public class HbaseImport {

    private static final String TABLE_QUERY1 = "Weekly Mean Swabs and Cured in Italy";
    private static final String TABLE_QUERY2 = "Continent Weekly Statistics";
    private static final String TABLE_QUERY3 = "Monthly Countries Clustering";

    // Query 1 table structure
    private static final String TABLE_QUERY1_CF = "Statistics";
    private static final String TABLE_QUERY1_C1 = "Cured";
    private static final String TABLE_QUERY1_C2 = "Swabs";

    // Query 2 table structure
    private static final String TABLE_QUERY2_CF = "Statistics";
    private static final String TABLE_QUERY2_C1 = "Mean";
    private static final String TABLE_QUERY2_C2 = "Standard Deviation";
    private static final String TABLE_QUERY2_C3 = "Minimum";
    private static final String TABLE_QUERY2_C4 = "Maximum";

    // Query 3 table structure
    private static final String TABLE_QUERY3_CF = "Clusters";
    private static final String TABLE_QUERY3_C1 = "Cluster 1";
    private static final String TABLE_QUERY3_C2 = "Cluster 2";
    private static final String TABLE_QUERY3_C3 = "Cluster 3";
    private static final String TABLE_QUERY3_C4 = "Cluster 4";

    public static void main(String[] args) {

        HBaseLightClient client = new HBaseLightClient();

        if (client.exists(TABLE_QUERY1)) {
            client.deleteTable(TABLE_QUERY1);
        }
        if (client.exists(TABLE_QUERY2)) {
            client.deleteTable(TABLE_QUERY2);
        }
        if (client.exists(TABLE_QUERY3)) {
            client.deleteTable(TABLE_QUERY3);
        }

        client.createTable(TABLE_QUERY1, TABLE_QUERY1_CF);
        client.createTable(TABLE_QUERY2, TABLE_QUERY2_CF);
        client.createTable(TABLE_QUERY3, TABLE_QUERY3_CF);

    }

    private static String importQuery1Result() {
        return null;
    }

    private static String importQuery2Result() {
        return null;
    }

    private static String importQuery3Result() {
        return null;
    }
}
