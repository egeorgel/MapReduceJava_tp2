package com.ece.edgar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Edgar on 09/10/2016.
 */
public class ProportionMaleFemaleReduce  extends
        Reducer<Text, IntWritable, Text, DoubleWritable> {

    public int sumFemal = 0;
    public int sumMale = 0;
    public int conteur = 0;

    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {


        if (text.toString().equals("f")) {
            for (IntWritable value : values) {
                sumFemal += value.get();
            }
            ++conteur;
        } else {
            for (IntWritable value : values) {
                sumMale += value.get();
            }
            ++conteur;
        }

        if (conteur == 2) {
            context.write(new Text("m"), new DoubleWritable( ((sumMale*1.0)/(sumFemal+sumMale))*100) );
            context.write(new Text("f"), new DoubleWritable( ((sumFemal*1.0)/(sumFemal+sumMale))*100) );
        }

    }
}