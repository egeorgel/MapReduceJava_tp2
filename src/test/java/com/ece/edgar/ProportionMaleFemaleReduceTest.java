package com.ece.edgar;

import org.apache.hadoop.io.DoubleWritable;
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
public class ProportionMaleFemaleReduceTest {

    ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;

    @Before
    public void setup() {
        ProportionMaleFemaleReduce reducer = new ProportionMaleFemaleReduce();

        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void INPUT_m4_f4_OUTPUT_m50_m50() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("m"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withInput(new Text("f"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("m"), new DoubleWritable(50))
                .withOutput(new Text("f"), new DoubleWritable(50))
                .runTest();
    }

    @Test
    public void INPUT_m2_f6_OUTPUT_m25_m75() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("m"), Arrays.asList(new IntWritable(3), new IntWritable(7)))
                .withInput(new Text("f"), Arrays.asList(new IntWritable(2), new IntWritable(3),new IntWritable(10)))
                .withOutput(new Text("m"), new DoubleWritable(40))
                .withOutput(new Text("f"), new DoubleWritable(60))
                .runTest();
    }

}
