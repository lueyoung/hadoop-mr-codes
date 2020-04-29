package com.young.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 压缩
        //compress("/tmp/hello.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        //compress("/tmp/hello.txt", "org.apache.hadoop.io.compress.GzipCodec");
        //compress("/tmp/hello.txt", "org.apache.hadoop.io.compress.DefaultCodec");

        //decompress("/tmp/hello.txt.bz2");
        //decompress("/tmp/hello.txt.gz");
        decompress("/tmp/hello.txt.deflate");
    }

    private static void decompress(String filename) throws IOException {
        // 1 压缩方式的检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));

        if (codec == null){
            System.out.println("can not process");
            return;
        }

        // 2 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        CompressionInputStream cis = codec.createInputStream(fis);
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));
        // 4 流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
        // 1 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        // 2 获取输出流
        Class classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        // 3 流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);
        // 4 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
