package com.ct.consumer.bean;

import com.ct.common.bean.Consumer;
import com.ct.common.constant.Names;
import com.ct.consumer.mapper.HBaseMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * 通话日志消费者对象
 */
public class CalllogConsumer implements Consumer {
    @Override
    public void consumer() {
        try {
            // 创建配置对象
            Properties properties = new Properties();
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            // 获取flume采集的数据
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

                // 关注主题
                consumer.subscribe(Collections.singletonList(Names.TOPIC.getValue()));

                // HBase数据访问对象
                HBaseMapper mapper = new HBaseMapper();
                mapper.init();

                // 消费数据
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        System.out.println(consumerRecord.value());
                        mapper.insertData(consumerRecord.value());
                        // 对象形式添加数据
//                        Calllog calllog = new Calllog(consumerRecord.value());
//                        mapper.insertData(calllog);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
