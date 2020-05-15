package com.wenl.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class  WcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        1.获取一行
        String line = value.toString();
//        2.切割
        String[] words = line.split(" ");
//        3.遍历输出
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
        }

    }
}


