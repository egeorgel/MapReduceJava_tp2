package com.ece.edgar;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Created by Edgar on 08/10/2016.
 */
public class FirstNameByOriginMapper extends
        Mapper<LongWritable, Text, Text, IntWritable>{

    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(";|\\r?\\n"); //split line whene new line or ;
        int conteur = 1;
        for (String str : csv) {
            if (conteur == 3) { // if it's a country name
                String[] countrys = str.split(", ");
                for (String country : countrys) {
                    word.set(country);
                    context.write(word, ONE);
                }
            } else if ( conteur == 4)
                conteur = 0;

            ++conteur;
        }
    }
}
