package output_and_metrics.graphics;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An influxDB client.
 * In order to use this class make sure to have in /etc/hosts the line "127.0.0.1  influxdb".
 */

public class InfluxDBClient {

    private static final String DB_URL = "influxdb:8086";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "password";

    private InfluxDB connection = null;

    public InfluxDB getConnection() {

        if (this.connection == null) {
            this.connection = InfluxDBFactory.connect(DB_URL, USERNAME, PASSWORD);
            this.connection.setLogLevel(InfluxDB.LogLevel.BASIC);
            Pong response = this.connection.ping();
            if (response.getVersion().equalsIgnoreCase("unknown")) {
                // error pinging server
                System.err.println("Error pinging influx db");
            }
        }
        return this.connection;
    }

    public void closeConnection() {

        if (!(this.connection == null)) {
            this.connection.close();
            this.connection = null;
        }
    }

    public void createDatabase(String dbName, String retentionTime) {
        InfluxDB connection = getConnection();
        connection.createDatabase(dbName);
        connection.createRetentionPolicy("defaultPolicy", dbName, retentionTime, 1, true);
    }

    public void insertPoints(String dbName, List<Double> slopes, List<String> countries,
                             int clusterAssignement, String month) {
        if (slopes.size() != countries.size()) {
            System.err.println("Data structure passed are not consistent in dimension");
            return;
        }
        try {
            DateFormat formatter = new SimpleDateFormat("MM-yyyy");
            long time = TimeUnit.MILLISECONDS.toDays(formatter.parse(month).getTime());
            BatchPoints batch = BatchPoints.database(dbName).retentionPolicy("defaultPolicy").build();
            Point newPoint;
            for (int i = 0; i < slopes.size(); i++) {
                newPoint = Point.measurement("clustering")
                        .time(time, TimeUnit.DAYS)
                        .addField("slope", slopes.get(i))
                        .addField("country", countries.get(i))
                        .addField("cluster", clusterAssignement)
                        .build();
                batch.point(newPoint);
            }
            getConnection().write(batch);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
