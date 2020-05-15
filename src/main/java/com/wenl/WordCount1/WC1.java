package com.wenl.WordCount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WC1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        获取对象实例
        Job job = Job.getInstance(new Configuration());
//        设置类路径
        job.setJarByClass(WC1.class);
        job.setMapperClass(WC1Mapper.class);
        job.setReducerClass(WC1Reducer.class);
//        设置Mapper 和 Reducer 的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        设置输入输出文件的路径
        FileInputFormat.setInputPaths( job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        提交job
        boolean b = job.waitForCompletion(true);
        System.exit( b? 0:1 );
    }

}

class WC1Mapper  extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text K = new Text();
    private  final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
            this.K.set(word);
            context.write(K , one);
        }

    }
}

class WC1Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        this.v.set(sum);
        context.write( key, v );
    }
}