package com.ece.edgar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Edgar on 10/10/2016.
 */
public class FirstNameByOriginTest  {


    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setup() {
        FirstNameByOriginMapper mapper = new FirstNameByOriginMapper();
        FirstNameByOriginReduce reducer = new FirstNameByOriginReduce();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void INPUT_indian1_OUTPUT_indian1() throws IOException, InterruptedException {
        Text value = new Text("len;m;english;0");

        mapReduceDriver.withInput(new LongWritable(0), value)
                       .withOutput(new Text("english"), new IntWritable(1))
                       .runTest();
    }

    @Test
    public void INPUT_indian11_english13_german1_OUTPUT_indian2_english4_german1() throws IOException, InterruptedException {
        Text value = new Text("lena;f;scandinavian, german, russian;0.06\n" +
                "leela;f;indian;0\n" +
                "len;m;english;0.40");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("english"), new IntWritable(1))
                .withOutput(new Text("german"), new IntWritable(1))
                .withOutput(new Text("indian"), new IntWritable(1))
                .withOutput(new Text("russian"), new IntWritable(1))
                .withOutput(new Text("scandinavian"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testFinal() throws IOException, InterruptedException {
        Text value = new Text("lena;f;scandinavian, german, russian;0.06\n" +
                              "leela;f;indian;0\n" +
                              "len;m;english;0.40");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withInput(new LongWritable(2), value)
                .withInput(new LongWritable(3), value)

                .withOutput(new Text("english"), new IntWritable(3))
                .withOutput(new Text("german"), new IntWritable(3))
                .withOutput(new Text("indian"), new IntWritable(3))
                .withOutput(new Text("russian"), new IntWritable(3))
                .withOutput(new Text("scandinavian"), new IntWritable(3))
                .runTest();
    }


}