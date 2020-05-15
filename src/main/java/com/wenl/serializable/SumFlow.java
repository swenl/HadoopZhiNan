package com.wenl.serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SumFlow {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        1.获取 job 对象
        Job job = Job.getInstance(new Configuration());

//        2.设置 class
        job.setJarByClass( SumFlow.class );
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(Reducer.class);

//        3.数据格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        4.路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

//        5.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit( b? 0 : 1 );


    }
}

class FlowMapper extends Mapper<LongWritable, Text, Text,FlowBean>{
    private Text k = new Text();
    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\\s+");

        this.k.set(splits[1]);
        this.v.setUpFlow(  Long.parseLong( splits[splits.length - 3] )  );
        this.v.setDownFlow(  Long.parseLong( splits[splits.length - 2] )  );

        context.write( k,v );
    }
}

class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long downSum = 0, upSum = 0;

        for (FlowBean val : values) {
            downSum += val.getDownFlow();
            upSum += val.getUpFlow();
        }
        v.setUpFlow(upSum);
        v.setDownFlow(downSum);

        context.write( key, v );
    }
}