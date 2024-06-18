package com.ls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StuAvg {

    public static class StuMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(",");
            if ( field.length == 5 && field[3].equals("必修")) {
                String Name = field[1] ;
                long score = Long.parseLong(field[4]);
                context.write(new Text(Name), new LongWritable(score));
            }
        }
    }

    public static class StuReduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;

            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                double average = (double) sum / (double) count;
                context.write(key, new DoubleWritable(average));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(StuMap.class);
        job.setMapperClass(StuMap.class);
        job.setReducerClass(StuReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

