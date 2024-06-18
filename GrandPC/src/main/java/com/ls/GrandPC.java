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
import java.util.ArrayList;
public class GrandPC {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] names = line.split(",");
            String child = names[0].trim()+"_child";
            String grand = names[1].trim()+"_parent";
            context.write(new Text(names[0].trim()), new Text(grand));
            context.write(new Text(names[1].trim()), new Text(child));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String grand = key.toString();
            ArrayList<String> parents = new ArrayList<>() ;
            ArrayList<String> children = new ArrayList<>() ;
            for (Text value : values) {
                String node = value.toString();
                String [] name = node.split("_") ;
                if ( name[1].equals("child") ) {
                    children.add(name[0].trim());
                }
                else if ( name[1].equals("parent") ) {
                    parents.add(name[0].trim());
                }
                else {
                    continue ;
                }
            }
            for (String child : children) {
                for (String parent : parents) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Map.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
