package com.young.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean bean = new TableBean();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件的名称
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String line = value.toString();
        if (name.startsWith("order")) {
            // 订单表
            String[] fields = line.split("\t");
            bean.setId(fields[0]);
            bean.setPid(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("order");
            k.set(fields[1]);
        }else {
            // 产品表
            String[] fields = line.split("\t");
            bean.setId("");
            bean.setPid(fields[0]);
            bean.setAmount(0);
            bean.setPname(fields[1]);
            bean.setFlag("pd");
            k.set(fields[0]);
        }
        context.write(k, bean);
    }
}
