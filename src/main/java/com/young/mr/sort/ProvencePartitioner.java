package com.young.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvencePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {
        int partition = 4;

        String prePhoneNum = value.toString().substring(0, 3);
        if ("136".equals(prePhoneNum)){
            partition = 0;
        }else if ("137".equals(prePhoneNum)){
            partition = 1;
        }else if ("138".equals(prePhoneNum)){
            partition = 2;
        }else if ("139".equals(prePhoneNum)){
            partition = 3;
        }

        return partition;
    }
}
