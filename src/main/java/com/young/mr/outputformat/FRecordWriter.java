package com.young.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosnefu;
    FSDataOutputStream fosother;
    public FRecordWriter(TaskAttemptContext job) {
        // 1 获取文件系统
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // 2 创建输出到nefu.log的输出流
            fosnefu = fs.create(new Path("/tmp/nefu.log"));
            // 3 others.log
            fosother = fs.create(new Path("/tmp/others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断key是否有nefu
        // true: ->nefu.log
        // false: ->others.log
        if (key.toString().contains("nefu")){
            fosnefu.write(key.toString().getBytes());
        }else {
            fosother.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosnefu);
        IOUtils.closeStream(fosother);
    }
}
