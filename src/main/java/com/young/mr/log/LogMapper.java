package com.young.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String line = value.toString();
        boolean res = parseLog(line, context);

        if (!res){
            return;
        }

        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        if (fields.length > 6){
            context.getCounter("map", "true").increment(1);
            return true;
        }else {
            context.getCounter("map", "false").increment(1);
            return false;
        }
    }
}
