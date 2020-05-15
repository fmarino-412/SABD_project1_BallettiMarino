package hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseLightClient {
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER_HOST = "localhost";
    private static final String HBASE_MASTER_PORT = "16000";
    private static final int HBASE_MAX_VERSIONS = 1;

    private Connection connection = null;

    public Connection getConnection() throws IOException, ServiceException {

        if (this.connection == null || this.connection.isClosed() || this.connection.isAborted()) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
            conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
            conf.set("hbase.master", HBASE_MASTER_HOST + ":" + HBASE_MASTER_PORT);

            HBaseAdmin.checkHBaseAvailable(conf);
            this.connection = ConnectionFactory.createConnection(conf);
        }

        return this.connection;
    }

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

    public void printTable(String tableName, String ...columns) {

        if (columns == null || (columns.length % 2 != 0)) {
            // Invalid usage of the function; columns should contain 2-ple in the
            // format: columnFamily, columnName
            System.err.println("Invalid usage of print table function; columns should " +
                    "contain 2-ple in the format: columnFamily, columnName");
            return;
        }

        try {

            Table table = getConnection().getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();

            for (int i = 0; i < (columns.length / 2); i++) {
                scan.addColumn(Bytes.toBytes(columns[i * 2]),
                        Bytes.toBytes(columns[(i * 2) + 1]));
            }

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
