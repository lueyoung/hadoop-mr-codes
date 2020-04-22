package com.young.mr.inputformat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    
        args = new String[]{"/Users/younglue/workspace-hadoop/input_inputformat", "/Users/younglue/workspace-hadoop/output"};
    
        Configuration conf = new Configuration();
        // 1 获取job对象
        Job job = Job.getInstance(conf);
    
        // 2 设置jar存储位置
        job.setJarByClass(SequenceFileDriver.class);
    
        // 3 关联Map和Reduce类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        
        // 设置输入的InputFormat类
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 设置输出的InputFormat类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    
        // 4 设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
    
        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
    
        // 如果不设置InputFormat，那么使用默认的，默认为TextInputFormat
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置：20m
        //CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
    
        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        // 7 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
