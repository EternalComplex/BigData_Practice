package com.ct.producer.bean;

import com.ct.common.bean.DataIn;
import com.ct.common.bean.DataOut;
import com.ct.common.bean.Producer;
import com.ct.common.util.DateUtil;
import com.ct.common.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile Boolean flag = true;   // volatile: 用来修饰会被不同线程访问和修改的变量

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    // 生产数据
    public void produce() {
        try {
            // TODO 读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);
            while (flag) {
                // TODO 从通讯录中随机查找2个电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                do {
                    call2Index = new Random().nextInt(contacts.size());
                } while (call1Index == call2Index);
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                // TODO 生成随机的通话时间
                String startDate = "20230101000000";
                String endDate = "20240101000000";

                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                // 通话时间
                long callTime = startTime + (long)((endTime - startTime) * Math.random());
                // 通话时间字符串
                String callTimeString = DateUtil.format(new Date(callTime), "yyyyMMddHHmmss");

                // TODO 生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(1800), 4);

                // TODO 生成通话记录
                Calllog log = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);

                // TODO 将通话记录写入到数据文件中
                out.write(log);

                // TODO 休眠500ms(即一秒写入两条数据)
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 关闭生产者
    public void close() throws IOException {
        if (in != null) in.close();
        if (out != null) out.close();
    }
}
