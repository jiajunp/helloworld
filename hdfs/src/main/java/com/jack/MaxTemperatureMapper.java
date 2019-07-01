package com.jack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: jiajunp
 * @Date: 2019/7/1 10:34
 * @Version 1.0
 */
public class MaxTemperatureMapper extends   Mapper<LongWritable, Text,Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemperature;
        if(line.charAt(87) == '+'){
            airTemperature = Integer.parseInt(line.substring(88,92));
        }else{
            airTemperature = Integer.parseInt((line.substring(87,92)));
        }

        String qualiy = line.substring(92,93);
        if(airTemperature != MISSING && qualiy.matches("[01459]")){
            context.write(new Text(year),new IntWritable(airTemperature));
        }

    }
}
