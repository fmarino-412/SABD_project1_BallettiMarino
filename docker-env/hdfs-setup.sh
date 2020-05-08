hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -mkdir /data
hdfs dfs -chmod -R 777 /data