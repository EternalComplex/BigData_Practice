package com.ct.analysis.mapper;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 分析数据mapper
 */
public class AnalysisBeanMapper extends TableMapper<AnalysisKey, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // rowKey = regionNum + call1 + callTime + call2 + duration + flag
        String rowKey = Bytes.toString(key.get());

        String[] values = rowKey.split("_");

        String call1 = values[1];
        String call2 = values[3];
        String callTime = values[2];
        String duration = values[4];

        String year = callTime.substring(0, 4);
        String month = callTime.substring(0, 6);
        String day = callTime.substring(0, 8);

        // 主叫用户 - 年
        context.write(new AnalysisKey(call1, year), new Text(duration));
        // 主叫用户 - 月
        context.write(new AnalysisKey(call1, month), new Text(duration));
        // 主叫用户 - 日
        context.write(new AnalysisKey(call1, day), new Text(duration));

        // 被叫用户 - 年
        context.write(new AnalysisKey(call2, year), new Text(duration));
        // 被叫用户 - 月
        context.write(new AnalysisKey(call2, month), new Text(duration));
        // 被叫用户 - 日
        context.write(new AnalysisKey(call2, day), new Text(duration));
    }
}
