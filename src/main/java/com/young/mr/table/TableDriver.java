package com.young.mr.table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        args = new String[]{"/Users/younglue/workspace-hadoop/inputtable", "/Users/younglue/workspace-hadoop/output"};

        Configuration conf = new Configuration();
        // 1 获取job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar存储位置
        job.setJarByClass(TableDriver.class);

        // 3 关联Map和Reduce类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 设置输入的InputFormat类
        //job.setInputFormatClass(.class);
        // 设置输出的InputFormat类
        //job.setOutputFormatClass(.class);


        // 4 设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 关联分区
        //job.setPartitionerClass(.class);
        //job.setNumReduceTasks(1);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 如果不设置InputFormat，那么使用默认的，默认为TextInputFormat
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置：20m
        //CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        //job.setCombinerClass(.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce端的分组
        //job.setGroupingComparatorClass(.class);
        // 将自定义的OutputFormat自建配置到job中
        //job.setOutputFormatClass(.class);

        // 7 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
