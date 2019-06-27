package com.example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GeneratorRecordReader extends RecordReader<Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneratorRecordReader.class);

  private final Text key = new Text();
  private final NullWritable value = NullWritable.get();

  private final int numRecordsToCreate;
  private int createdRecords = 0;

  private final int numberOfYears;

  private Random random = new Random();

  private List<String> names = new ArrayList<>();
  private List<String> cities = new ArrayList<>();

  GeneratorRecordReader(int numRecordsToCreate, int numberOfYears) {
    this.numRecordsToCreate = numRecordsToCreate;
    this.numberOfYears = numberOfYears;
  }


  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

    if (numRecordsToCreate < 0) {
      throw new InvalidParameterException("The number of records per task is not set.");
    }

    URI[] files = context.getCacheFiles();
    if (files.length == 0) {
      throw new InvalidParameterException(
              "The list of the random names and cities is not set in cache.");
    } else {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      names = readLines(fs, files[0]);
      cities = readLines(fs, files[1]);
    }

    LOGGER.debug("GeneratorRecordReader is initialized");
  }

  private List<String> readLines(FileSystem fs, URI uri) throws IOException {

    List<String> lines = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
    String line;
    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }
    reader.close();

    if (lines.size() == 0) {
      throw new IOException(uri + " is empty");
    }

    return lines;
  }

  @Override
  public boolean nextKeyValue() {

    if (createdRecords < numRecordsToCreate) {

      String sb = getRandomName() + "," + getRandomCity() + "," + getRandomBirthday();
      this.key.set(sb);
      createdRecords++;

      LOGGER.debug("next key is {}", sb);

      return true;
    }

    return false;
  }

  private String getRandomName() {
    return names.get(Math.abs(random.nextInt()) % names.size());
  }

  private String getRandomCity() {
    return cities.get(Math.abs(random.nextInt()) % cities.size());
  }

  private LocalDate getRandomBirthday() {
    return LocalDate.now().minus(Period.ofDays((new Random().nextInt(365 * numberOfYears))));
  }


  @Override
  public Text getCurrentKey() {
    return this.key;
  }

  @Override
  public NullWritable getCurrentValue() {
    return this.value;
  }

  @Override
  public float getProgress() {
    return (float) createdRecords / (float) numRecordsToCreate;
  }

  @Override
  public void close() {

  }
}
