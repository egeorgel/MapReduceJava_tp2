package com.ece.edgar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Edgar on 09/10/2016.
 */
public class NumberOfFirstNameByNumberOfOriginReducerTest {
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setup() {
        SumReduce reducer = new SumReduce();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void INPUT_indian1_OUTPUT_indian1() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new IntWritable(1)))
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void INPUT_indian11_OUTPUT_indian2() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("1"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void INPUT_indian123_OUTPUT_indian6() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new IntWritable(1),new IntWritable(2) ,new IntWritable(3)))
                .withOutput(new Text("1"), new IntWritable(6))
                .runTest();
    }

    @Test
    public void INPUT_indian11_english13_german1_OUTPUT_indian2_english4_german1() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withInput(new Text("2"), Arrays.asList(new IntWritable(1), new IntWritable(3)))
                .withInput(new Text("3"), Arrays.asList(new IntWritable(1)))
                .withOutput(new Text("1"), new IntWritable(2))
                .withOutput(new Text("2"), new IntWritable(4))
                .withOutput(new Text("3"), new IntWritable(1))
                .runTest();
    }

}

