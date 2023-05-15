package com.ct.consumer.mapper;

import com.ct.common.bean.BaseMapper;
import com.ct.common.constant.Names;
import com.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase数据访问对象
 */
public class HBaseMapper extends BaseMapper {
    // 初始化
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.NAMESPACE.getValue(), Names.TABLE.getValue(), "com.ct.consumer.coprocessor.InsertCalleeCoprocessor", Integer.parseInt(Names.REGION_COUNT.getValue()), Names.CF_CALLER.getValue(), Names.CF_CALLEE.getValue());

        end();
    }

    // 插入对象
    public void insertData(Calllog log) throws IOException, IllegalAccessException {
        log.setRowKey(genRegionNum(log.getCall1(), log.getCallTime()) + "_" + log.getCall1() + "_" + log.getCallTime() + "_" + log.getCall2() + "_" + log.getDuration());
        putData(log);
    }

    // 插入数据
    public void insertData(String value) throws IOException {
        // TODO 将通话日志保存到hbase表中
        // 获取通话日志数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String callTime = values[2];
        String duration = values[3];

        // 创建数据对象
        /*
        * rowKey的设计:
        *   1.长度原则: 最大值64kb,推荐10~100byte(最好为8的倍数，能短则短)
        *   2.唯一原则: rowKey应具备唯一性
        *   3.散列原则: (防止出现rowKey总是按照规律分到同一分区的情况)
        *       盐值散列: 在rowKey添加随机数(不能使用时间戳直接作为rowKey)
        *       字符串反转: 常用于电话号码等有一定规律的字符串
        *       计算分区号: hashMap
        * */
        // rowKey = regionNum + call1 + time + call2 + duration
        String rowKey = genRegionNum(call1, callTime) + "_" + call1 + "_" + callTime + "_" + call2 + "_" + duration + "_1";
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()), Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()), Bytes.toBytes("flag"), Bytes.toBytes(1));

        // 以下代码通过hbase自带的协处理器完成，以降低代码的耦合性
        // 额外添加被叫用户的数据，以便根据被叫号码进行查询操作
//        String calleeRowKey = genRegionNum(call2, callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
//        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
//        calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("call1"), Bytes.toBytes(call2));   // 主叫与被叫号码互换
//        calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("call2"), Bytes.toBytes(call1));
//        calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
//        calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("duration"), Bytes.toBytes(duration));
//        calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("flag"), Bytes.toBytes(0));

        // 保存数据
        putData(Names.NAMESPACE.getValue(), Names.TABLE.getValue(), put);
//        putData(Names.NAMESPACE.getValue(), Names.TABLE.getValue(), calleePut);
    }
}
