package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class GeneratorDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneratorDriver.class);

  private static final String NUM_TASKS = "customer.generator.num.tasks";
  private static final String NUM_RECORDS_PER_TASK = "customer.generator.num.records.per.task";
  private static final String NUM_OF_YEARS = "customer.generator.num.of.years";

  public static class CustomerInputFormat extends InputFormat<Text, NullWritable> {


    @Override
    public List<InputSplit> getSplits(JobContext context) {
      int numberOfSplits = context.getConfiguration().getInt(NUM_TASKS, 1);
      LOGGER.debug("number of splits {}", numberOfSplits);

      List<InputSplit> splits = new ArrayList<>();
      for (int i = 0; i < numberOfSplits; i++) {
        splits.add(new FakeInputSplit());
      }
      return splits;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {

      LOGGER.debug("creating record reader");
      int numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
      int numberOfYears = context.getConfiguration().getInt(NUM_OF_YEARS, 10);

      return new GeneratorRecordReader(numRecordsToCreate, numberOfYears);
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 4) {
      LOGGER.info("Usage: GeneratorDriver <size> <output-folder> <names> <cities>");
      return;
    }

    int size = Integer.parseInt(otherArgs[0]);
    if (size <= 0) {
      LOGGER.info("The size must be greater than 0!");
      return;
    }

    Path outputDir = new Path(otherArgs[1]);

    LOGGER.debug("size={}, output={}", size, outputDir);

    Job job = Job.getInstance(conf, "GeneratorDriver");
    job.setJarByClass(GeneratorDriver.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(CustomerInputFormat.class);

    job.getConfiguration().setInt(NUM_TASKS, 1);
    job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, size);

    TextOutputFormat.setOutputPath(job, outputDir);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);


    job.addCacheFile(new Path(otherArgs[2]).toUri());
    job.addCacheFile(new Path(otherArgs[3]).toUri());


    LOGGER.info("Job configuration is done!");
    System.exit(job.waitForCompletion(true) ? 0 : 2);
  }
}
