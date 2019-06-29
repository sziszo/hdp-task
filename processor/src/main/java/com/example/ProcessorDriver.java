package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProcessorDriver implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorDriver.class);

  private static final String ZOOKEEPER_QUORUM = "sandbox-hdp.hortonworks.com";
  private static final String PHOENIX_JDBC_URL = "jdbc:phoenix:" + ZOOKEEPER_QUORUM;

  private static final String APP_NAME = "ProcessorDriver";
  private static final String COLUMN_FAMILY = "frequency";
  private static final String COUNT_COLUMN = "Count";

  private final String inputFolder;
  private final String outputTableName;

  private ProcessorDriver(String inputFolder, String outputTableName) {
    this.inputFolder = inputFolder;
    this.outputTableName = outputTableName;
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Started ProcessorDriver job");

      String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
      if (otherArgs.length != 2) {
        LOGGER.info("Usage: ProcessorDriver input_folder table_name");
        return;
      }

//      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

      ProcessorDriver processorDriver = new ProcessorDriver(otherArgs[0], otherArgs[1]);
      processorDriver.process();

    } catch (Exception e) {
      LOGGER.error("An error occurred!", e);
    }

  }

  private void process() throws IOException, SQLException {

    Configuration configuration = initConfig();

    if (!isHBaseAvailable(configuration)) return;

    //init output table
    dropPhoenixTable();
    createHBaseTable(configuration);

    // Spark Config
    SparkConf conf = new SparkConf().setAppName(APP_NAME);
    try (JavaSparkContext sc = new JavaSparkContext(conf);
         SparkSession sparkSession = SparkSession.builder()
                 .appName(APP_NAME)
                 .config(conf)
                 .getOrCreate()) {

      process(sparkSession, configuration);

    }

    //creating Phoenix table
    createPhoenixTable();
  }

  private Configuration initConfig() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    return configuration;
  }

  private boolean isHBaseAvailable(Configuration configuration) {
    try {
      HBaseAdmin.checkHBaseAvailable(configuration);
      LOGGER.info("------------------HBase is running!------------------");
    } catch (Exception e) {
      LOGGER.error("An error occurred while checking the availability of HBase!", e);
      return false;
    }
    return true;
  }

  private void dropPhoenixTable() throws SQLException {
    try (java.sql.Connection con = DriverManager.getConnection(PHOENIX_JDBC_URL);
         Statement stm = con.createStatement()) {

      stm.executeUpdate("drop table \"" + outputTableName + "\"");
      con.commit();

    } catch (TableNotFoundException e) {
      //nothing to do
    }
  }

  private void createHBaseTable(Configuration configuration) throws IOException {

    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Admin admin = connection.getAdmin()) {

      TableName tableName = TableName.valueOf(outputTableName);

      // drop the output table if exists
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }

      // creating the output table
      admin.createTable(
              new HTableDescriptor(tableName)
                      .addFamily(
                              new HColumnDescriptor(COLUMN_FAMILY)
                                      .setBloomFilterType(BloomType.NONE)
                                      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                      )
      );

    }
  }

  private void process(SparkSession sparkSession, Configuration configuration) throws IOException {
    Job newAPIJobConfiguration = Job.getInstance(configuration);
    newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTableName);
    newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

    JavaPairRDD<ImmutableBytesWritable, Put> puts = sparkSession
            .read()
//            .option("header", "true")
//            .option("inferSchema", true)
            .csv(inputFolder)
            .javaRDD()
            .map(this::createKey)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .mapToPair(key -> new Tuple2<>(key, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(tuple -> {

              Put put = new Put(createRowKey(tuple._1));
              put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COUNT_COLUMN), Bytes.toBytes(tuple._2));
              return new Tuple2<>(new ImmutableBytesWritable(), put);

            })
            .cache();

    long rowCount = puts.count();

    LOGGER.info("The number of rows to be inserted {}", rowCount);

    // saving the result into the output table
    puts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
  }

  private Optional<String> createKey(Row row) {
    return IntStream.range(0, 3)
            .mapToObj(row::getString)
            .map(String::trim)
            .reduce((s, s2) -> s + ":" + s2);
  }

  private byte[] createRowKey(String key) {
    return Stream
            .of(key.split(":"))
            .map(Bytes::toBytes)
            .reduce((bytes, bytes2) -> {

              byte[] result = new byte[bytes.length + bytes2.length + 1];
              System.arraycopy(bytes, 0, result, 0, bytes.length);
              result[bytes.length] = QueryConstants.SEPARATOR_BYTE;
              System.arraycopy(bytes2, 0, result, bytes.length + 1, bytes2.length);

              return result;
            })
            .orElseGet(() -> Bytes.toBytes(key));
  }

  private void createPhoenixTable() throws SQLException {

    try (java.sql.Connection con = DriverManager.getConnection(PHOENIX_JDBC_URL);
         Statement stm = con.createStatement()) {

      String createStm = "create table \"" +
              outputTableName +
              "\" ( first_name varchar not null, last_name varchar not null, location varchar not null, \"" + COLUMN_FAMILY + "\"." + COUNT_COLUMN + " UNSIGNED_INT, constraint pk primary key (first_name, last_name, location))";

      LOGGER.debug("createStm={}", createStm);

      stm.executeUpdate(createStm);

      con.commit();
    }
  }

}
