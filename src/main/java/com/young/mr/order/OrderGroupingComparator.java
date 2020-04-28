package com.young.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int res;

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        if (aBean.getOrder_id() > bBean.getOrder_id()){
            res = 1;
        }else if (aBean.getOrder_id() < bBean.getOrder_id()){
            res = -1;
        }else {
            res = 0;
        }

        return res;
    }
}
