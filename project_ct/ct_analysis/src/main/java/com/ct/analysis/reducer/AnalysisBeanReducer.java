package com.ct.analysis.reducer;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据reducer
 */
public class AnalysisBeanReducer extends Reducer<AnalysisKey, Text, AnalysisKey, AnalysisValue> {
    @Override
    protected void reduce(AnalysisKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;    // 通话次数
        int sumDuration = 0;    // 通话时长

        for (Text value : values) {
            sumCall++;
            sumDuration += Integer.parseInt(value.toString());
        }

        context.write(key, new AnalysisValue(sumCall, sumDuration));
    }
}
