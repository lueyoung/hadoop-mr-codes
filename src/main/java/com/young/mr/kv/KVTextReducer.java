package com.young.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// root 1
// root 2
public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //
        int sum = 0;
        // 1 累加求和
        for (IntWritable value: values){
            sum += value.get();
        }
        // 2 写出
        v.set(sum);
        context.write(key, v);
    }
}
