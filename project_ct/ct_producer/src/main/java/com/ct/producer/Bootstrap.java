package com.ct.producer;

import com.ct.common.bean.Producer;
import com.ct.producer.bean.LocalFileProducer;
import com.ct.producer.io.LocalFileDataIn;
import com.ct.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("参数不正确！请按照指定格式传递：java -jar Produce.jar inputPath outputPath");
            System.exit(1);
        }

        // TODO 构建生产者对象
        Producer producer = new LocalFileProducer();

//        producer.setIn(new LocalFileDataIn("D:\\BigData_Practice\\project_ct\\ct_producer\\src\\main\\resources\\input\\contact.log"));
//        producer.setOut(new LocalFileDataOut("D:\\BigData_Practice\\project_ct\\ct_producer\\src\\main\\resources\\output\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        // TODO 生产数据
        producer.produce();

        // TODO 关闭生产者对象
        producer.close();
    }
}
