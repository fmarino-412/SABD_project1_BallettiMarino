# SABD 2019/2020 first project
Authors: Marco Balletti, Francesco Marino

## Project structure descritption:

### Results
Folder containing CSV files:

1. `query1_output.csv` containing the output of the first query,
2. `query2_output.csv` containing the output of the second query,
3. `query3_mllib_output.csv` containing the output of the third query performed using the mllib k-means implementation,
4. `query3_naive_output.csv` containing the output of the third query performed using a naive implementation of the Lloyd k-means algorithm.

### Documentation
Folder containing benchmark results, project report and project presentation slides.
### docker-env
Folder containing scripts and file for a container based execution of the project architecture.

_Further explainations about container execution are in a readme file inside the directory._

### src
this directory contains in its subdirectories Java code for:

1. moving data from HDFS to Spark,
2. queries execution,
3. data export to HDFS,
4. moving data from HDFS to Hbase,
5. moving data from HDFS to InfluxDB.



## Java Project structure description:

It is recommended to open the entire directory with an IDE for better code navigation. Java project part was developed using JetBrains' IntelliJ IDEA.

### query1 package

This package contains:

* `Query1Preprocessing.java` implementing the data preprocessing for the first query execution with both Spark Core and Spark SQL,
* `Query1Main.java` implementing the first query resolution using Spark Core Transformations and Actions,
* `Query1SparkSQL.java` implementing the first query resolution using Spark SQL (preprocesing executed using Spark Core Transformations and Actions).

### query2 package

This package contains:

* `Query2Preprocessing.java` implementing the data preprocessing for the second query execution with both Spark Core and Spark SQL,
* `Query2Main.java` implementing the second query resolution using Spark Core Transformations and Actions,
* `Query2SparkSQL.java` implementing the second query resolution using Spark SQL (preprocesing executed using Spark Core Transformations and Actions),
* `CountryDataQuery2.java` structure used to incapsulate data to pass between Spark Transformations/Actions.

### query3 package

This package contains:

* `Query3Main.java` implementing the third query resolution (excluding the clustering part) using Spark Core Transformations and Actions,
* `CountryDataQuery3.java` structure used to incapsulate data to pass between Transformations/Actions.

### utility package

This package contains classes needed for queries execution support, in particular:

* `Boundary.java` structure to define polygonal fences representing geographical continents,
* `Continents.java` containing mapping of geographical continents and boundary structures,
* `Codes.java` enum containing ISO3 country codes each mapped to the corresponding continent,
* `ContinentDecoder.java` implementing different logics for continent detection starting from geographical coordinates,
* `ClusteringUtility.java` implementing naive and mllib k-means clustering versions,
* `GeoCoordinate.java` encapsulating latitude and longitude in a single object,
* `IOUtility.java` implementing logic for HDFS communication,
* `QueryUtility.java` containing methods for data conversion and dataset translation.

### output\_and\_metrics package

This package contains classes needed for performing banchmark and exporting queries results, in particular:

* `CSVOutputFormatter.java` executes queries and export results from HDFS to `.csv` files inside the `Results` directory,
* `MultiRunBenchmark.java` performs a multi-start for every query execution printing every run execution time.

### output\_and\_metrics.hbase package

This package contains classes needed to export queries results from HDFS to HBase, in particular:

* `HBaseLightClient.java` implementing basic methods for datastore interaction,
* `HBaseImport.java` implementing methods to get query outputs from HDFS and putting in HBase after format translation.

### output\_and\_metrics.graphics package

This package contains classes needed to export queries result from HDFS to InfluxDB in order to be graphically represented using Grafana, in particular:

* `InfluxDBClient.java` implementing basic methods for datastore interaction,
* `InfluxDBImport.java` implementing methods to get query outputs from HDFS and putting in InfluxDB after format translation.