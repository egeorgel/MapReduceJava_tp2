package com.ece.edgar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Edgar on 09/10/2016.
 */
public class NumberOfFirstNameByNumberOfOriginMapperTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setup() {
        NumberOfFirstNameByNumberOfOriginMapper mapper = new NumberOfFirstNameByNumberOfOriginMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void INPUT_len_m_english_0_X1_OUTPUT_english_1_X1() throws IOException, InterruptedException {
        Text value = new Text("len;m;english;0");

        mapDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void INPUT_len_m_english_0_X2_OUTPUT_english_1_X2() throws IOException, InterruptedException {
        Text value = new Text("len;m;english;0\n" +
                              "len;m;english;0");

        mapDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1"), new IntWritable(1))
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void INPUT_leela_f_indian_0_AND_len_m_english_0_OUTPUT_indian1_english1() throws IOException, InterruptedException {
        Text value = new Text("leela;f;indian;0\n" +
                              "len;m;english;0");

        mapDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("1"), new IntWritable(1))
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void INPUT_lena_f_scandinavianGermanRussian_0_AND_leela_f_indian_0_AND_len_m_english_0_OUTPUT_scandinavian1_german1_russian1_indian1_english1() throws IOException, InterruptedException {
        Text value = new Text("lena;f;scandinavian, german, russian;0.06\n" +
                            "leela;f;indian;0\n" +
                            "len;m;english;0.40");

        mapDriver.withInput(new LongWritable(0), value)
                .withOutput(new Text("3"), new IntWritable(1))
                .withOutput(new Text("1"), new IntWritable(1))
                .withOutput(new Text("1"), new IntWritable(1))
                .runTest();
    }

}