package com.jack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: jiajunp
 * @Date: 2019/7/1 10:48
 * @Version 1.0
 */
public class MaxTemperatureReducer  extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;

        for (IntWritable value : values){
            maxValue = Math.max(maxValue,value.get());
        }

        context.write(key,new IntWritable(maxValue));
    }
}
