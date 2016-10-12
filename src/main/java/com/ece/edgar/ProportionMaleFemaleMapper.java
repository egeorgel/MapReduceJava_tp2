package com.ece.edgar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Edgar on 09/10/2016.
 */
public class ProportionMaleFemaleMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(";|\\r?\\n"); //split line whene new line or ;
        int conteur = 1;
        for (String str : csv) {
            if (conteur == 2) { // if it's a country name
                word.set(str);
                context.write(word, ONE);
            } else if ( conteur == 4)
                conteur = 0;

            ++conteur;
        }
    }
}
