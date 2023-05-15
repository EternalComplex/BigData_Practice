package com.ct.consumer.coprocessor;

import com.ct.common.bean.BaseMapper;
import com.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.Optional;

/**
 * 使用协处理器保存被叫用户的数据
 * 协处理器的使用：
 *  1.创建类
 *  2.让表知道协处理类（和表有关联）
 *  3.将此项目打包分发到服务器集群中所有hbase的lib目录下(连同依赖的项目)
 */
public class InsertCalleeCoprocessor implements RegionObserver, RegionCoprocessor {
    /*
        方法的命名规则
         login
         logout
         prePut
         doPut: 模板方法设计模式
            存在父子类，父类搭建算法的骨架，子类重写算法的细节
         postPut
     */

    // hbase2.x必须重写该方法
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    // 保存主叫用户数据之后，由hbase自动保存被叫用户数据
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit) throws IOException {
        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(Names.NAMESPACE.getValue(), Names.TABLE.getValue()));

        // 获取主叫用户的rowKey
        String rowKey = Bytes.toString(put.getRow());
        String[] values = rowKey.split("_");

        CoprocessorMapper mapper = new CoprocessorMapper();

        String flag = values[5];
        if ("1".equals(flag)) { // 为防止无限循环调用postPut，需要判断是否是主叫用户
            String call1 = values[1];
            String call2 = values[2];
            String callTime = values[3];
            String duration = values[4];
            String calleeRowKey = mapper.getRegionNum(call2, callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";

            Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
            calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("call1"), Bytes.toBytes(call2));   // 主叫与被叫号码互换
            calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
            calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()), Bytes.toBytes("flag"), Bytes.toBytes("0"));

            table.put(calleePut);
        }

        table.close();
    }

    private static class CoprocessorMapper extends BaseMapper {
        public int getRegionNum(String tel, String time) {
            return super.genRegionNum(tel, time);
        }
    }
}
