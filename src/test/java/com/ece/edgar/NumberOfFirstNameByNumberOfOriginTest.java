package com.ece.edgar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Edgar on 11/10/2016.
 */
public class NumberOfFirstNameByNumberOfOriginTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setup() {
        NumberOfFirstNameByNumberOfOriginMapper mapper = new NumberOfFirstNameByNumberOfOriginMapper();
        FirstNameByOriginReduce reducer = new FirstNameByOriginReduce();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void INPUT_indian1_OUTPUT_11() throws IOException, InterruptedException {
        Text value = new Text("len;m;english;0");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void INPUT_indian11_english13_german1_OUTPUT_31_11() throws IOException, InterruptedException {
        Text value = new Text("lena;f;scandinavian, german, russian;0.06\n" +
                "leela;f;indian;0\n" +
                "len;m;english;0.40");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1"), new IntWritable(2))
                .withOutput(new Text("3"), new IntWritable(1))
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

                .withOutput(new Text("1"), new IntWritable(6))
                .withOutput(new Text("3"), new IntWritable(3))
                .runTest();
    }


}
