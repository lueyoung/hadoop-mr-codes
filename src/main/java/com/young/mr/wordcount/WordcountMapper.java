package com.young.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// map阶段
// key in:
// value in:
// key out:
// value out:
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println(key.toString());
        // root root
        // 1 将Text转化为String
        String line = value.toString();
        // 2 根据空格进行切分
        String[] words = line.split(" ");
        // 3 遍历输出
        for (String word: words){
            // root
            k.set(word);
            // 1
            //v.set(1);
            context.write(k, v);
        }
    }
}
