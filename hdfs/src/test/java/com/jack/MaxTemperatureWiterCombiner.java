package com.jack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.print.PrinterJobWrapper;

import java.io.IOException;

/**
 * @Author: jiajunp
 * @Date: 2019/7/1 19:33
 * @Version 1.0
 */
public class MaxTemperatureWiterCombiner  {

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("Usage: MaxTemperatureWiterCombiner <input path> <OUTPUT PATH>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MaxTemperatureWiterCombiner.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)? 0:1);



    }
}
