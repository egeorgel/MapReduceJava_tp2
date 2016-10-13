package com.ece.edgar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Edgar on 12/10/2016.
 */
public class ProportionMaleFemaleTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setup() {
        ProportionMaleFemaleMapper mapper = new ProportionMaleFemaleMapper();
        ProportionMaleFemaleReduce reducer = new ProportionMaleFemaleReduce();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void INPUT_m1_f1_OUTPUT_m50_f50() throws IOException, InterruptedException {
        Text value = new Text("len;m;english;0\n"+
                                "len;f;english;0");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("m"), new DoubleWritable(50))
                .withOutput(new Text("f"), new DoubleWritable(50))
                .runTest();
    }

    @Test
    public void INPUT_m1_f2_OUTPUT_m33_f67() throws IOException, InterruptedException {
        Text value = new Text("lena;f;scandinavian, german, russian;0.06\n" +
                                "leela;f;indian;0\n" +
                                "len;m;english;0.40");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("m"), new DoubleWritable(33.33333333333333))
                .withOutput(new Text("f"), new DoubleWritable(66.66666666666666))
                .runTest();
    }

    @Test
    public void testFinal_15m_6f_OUTPUT_m72_f28() throws IOException, InterruptedException {
        Text value = new Text("lena;f,f;scandinavian, german, russian;0.06\n" +
                "len;m,m;english;0.40\n"+
                "len;m;english;0.40\n"+
                "len;m;english;0.40\n"+
                "len;m;english;0.40");

        mapReduceDriver.withInput(new LongWritable(0), value)
                .withInput(new LongWritable(2), value)
                .withInput(new LongWritable(3), value)

                .withOutput(new Text("m"), new DoubleWritable(71.42857142857143))
                .withOutput(new Text("f"), new DoubleWritable(28.57142857142857))
                .runTest();
    }

}
