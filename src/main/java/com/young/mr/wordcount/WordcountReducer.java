package com.young.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //
        // root,1
        // root,1
        // 1 累加和
        int sum = 0;
        for (IntWritable value: values){
            sum += value.get();
        }
        // 2 写出
        v.set(sum);
        context.write(key, v);
    }
}
