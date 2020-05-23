package output_and_metrics.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * An hbase client containing the required functions to export data from HDFS to the datastore
 * In order to use this class make sure to have in /etc/hosts the line "127.0.0.1  hbase".
 */

public class HBaseLightClient {

    private static final String ZOOKEEPER_HOST = "hbase";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER = "hbase:16000";
    private static final int HBASE_MAX_VERSIONS = 1;

    private Connection connection = null;

    /**
     * Used to return an instance of connection. If the current one is not active or does not exist, a new
     * connection is created and the current instance is updated to the new one.
     * @return Connection to hbase
     * @throws IOException
     * @throws ServiceException
     */
    public Connection getConnection() throws IOException, ServiceException {

        if (this.connection == null || this.connection.isClosed() || this.connection.isAborted()) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
            conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
            conf.set("hbase.master", HBASE_MASTER);

            HBaseAdmin.checkHBaseAvailable(conf);
            this.connection = ConnectionFactory.createConnection(conf);
        }

        return this.connection;
    }

    /**
     * Closes the current hbase connection, it must be called when no other operation has to be performed on the
     * datastore.
     */
    public void closeConnection() {

        if (!(this.connection == null || this.connection.isClosed() || this.connection.isAborted())) {
            try {
                this.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                this.connection = null;
            }
        }
    }

    /**
     * Creates a new table with the requested name and column families.
     * @param tableName name of the new table
     * @param columnFamilies column families (one or more) which the new table must be formed of
     * @return true if the new table has been created, false elsewhere
     */
    public boolean createTable(String tableName, String ... columnFamilies) {

        try {
            // must assume admin privileges
            Admin admin = getConnection().getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            // add column families
            for (String columnFamily : columnFamilies) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                columnDescriptor.setMaxVersions(HBASE_MAX_VERSIONS);
                tableDescriptor.addFamily(columnDescriptor);
            }

            admin.createTable(tableDescriptor);
            return true;
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Checks if a table with the specified name already exists.
     * @param tableName name of the table whose existence is to be verified
     * @return true if the specified table exists, false elsewhere
     */
    public boolean exists(String tableName) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName table = TableName.valueOf(tableName);
            return admin.tableExists(table);
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Deletes the specified table.
     * @param tableName name of the table to delete
     * @return true if the deletion has been completed, false elsewhere
     */
    public boolean deleteTable(String tableName) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName table = TableName.valueOf(tableName);

            // disable table before dropping
            admin.disableTable(table);

            // drop table
            admin.deleteTable(table);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Puts a new row in the specified table.
     * @param tableName name of the table which the put operation must be performed on
     * @param rowKey key of the new row
     * @param columns column family, column name and value that the new row must contain, for every value to add the
     *                3-ple (column family, column name, value) must be specified
     * @return true if the insertion has been completed, false elsewhere
     */
    public boolean put(String tableName, String rowKey, String ...columns) {

        if (columns == null || (columns.length % 3 != 0)) {
            // Invalid usage of the function; columns should contain 3-ple in the
            // format: columnFamily, columnName, value
            return false;
        }

        try {

            Table hTable = getConnection().getTable(TableName.valueOf(tableName));

            Put row = new Put(Bytes.toBytes(rowKey));

            for (int i = 0; i < (columns.length / 3); i++) {
                row.addColumn(Bytes.toBytes(columns[i * 3]),
                        Bytes.toBytes(columns[(i * 3) + 1]),
                        Bytes.toBytes(columns[(i * 3) + 2]));
            }

            // Put row in table
            hTable.put(row);

            hTable.close();

            return true;
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Shows the specified table structure (columns for every table row)
     * @param tableName name of the table whose structure is to be printed
     */
    public void printTable(String tableName) {

        try {

            Table table = getConnection().getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();

            ResultScanner scanner = table.getScanner(scan);

            // Reading values from scan result
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                System.out.println("Found row : " + result);
            }

            scanner.close();

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            System.err.println("Could not print the requested table");
        }
    }
}
