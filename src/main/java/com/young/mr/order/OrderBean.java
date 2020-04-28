package com.young.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id; // 订单id
    private double price; // 价格

    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        // 先按订单id升序排序，
        // 如果订单id相同，则按照价格降序排序
        int res;

        if (order_id > o.getOrder_id()){
            res = 1;
        }else if (order_id < o.getOrder_id()){
            res = -1;
        }else {
            if (price > o.getPrice()){
                res = -1;
            }else if (price < o.getPrice()){
                res = 1;
            }else {
                res = 0;
            }
        }

        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
