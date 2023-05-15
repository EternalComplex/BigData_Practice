package com.ct.consumer;

import com.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * 启动消费者
 * 使用kafka消费flume采集的数据
 * 将数据存储到Hbase中去
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        // TODO 创建消费者
        CalllogConsumer consumer = new CalllogConsumer();

        // TODO 消费数据
        consumer.consumer();

        // TODO 关闭资源
        consumer.close();
    }
}
