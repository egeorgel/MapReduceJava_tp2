package com.ece.edgar;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Hello world!
 *
 */
public class App {

    public static int _job1IsRunning = 0;
    public static int _job2IsRunning = 0;
    public static int _job3IsRunning = 0;

    public static void main(String[] args) {

        try {
            // 1 M/P
            firsNameByOriginJob(args[0], args[1]);
            // 2 M/P
            NumberOfPirstNameByNumberOfOrigin(args[0], args[2]);
            // 3 M/P
            ProportionMalFemale(args[0], args[3]);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void firsNameByOriginJob(String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(input);
        Path outputDir = new Path(output);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, "FirstNameByOrigin");
        job.setJarByClass(FirstNameByOriginMapper.class);

        // Setup MapReduce && combiner
        job.setMapperClass(FirstNameByOriginMapper.class);
        job.setCombinerClass(SumReduce.class);
        job.setReducerClass(SumReduce.class);
        job.setNumReduceTasks(1);

        jobSpecification(inputPath, outputDir, conf, job);


        // Execute job
        _job1IsRunning = job.waitForCompletion(true) ? 0 : 1;

    }


    public static void NumberOfPirstNameByNumberOfOrigin(String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(input);
        Path outputDir = new Path(output);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, "NumberOfPirstNameByNumberOfOrigin");
        job.setJarByClass(NumberOfFirstNameByNumberOfOriginMapper.class);

        // Setup MapReduce && combiner
        job.setMapperClass(NumberOfFirstNameByNumberOfOriginMapper.class);
        job.setCombinerClass(SumReduce.class);
        job.setReducerClass(SumReduce.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        jobSpecification(inputPath, outputDir, conf, job);

        // Execute job
        _job2IsRunning = job.waitForCompletion(true) ? 0 : 1;
    }

    public static void ProportionMalFemale(String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(input);
        Path outputDir = new Path(output);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, "ProportionMalFemale");
        job.setJarByClass(ProportionMaleFemaleMapper.class);

        // Setup MapReduce
        job.setMapperClass(ProportionMaleFemaleMapper.class);
        job.setCombinerClass(SumReduce.class);
        job.setReducerClass(ProportionMaleFemaleReduce.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        jobSpecification(inputPath, outputDir, conf, job);

        // Execute job
        _job3IsRunning = job.waitForCompletion(true) ? 0 : 1;
    }


    private static void jobSpecification(Path inputPath, Path outputDir, Configuration conf, Job job) throws IOException {
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
    }


}
