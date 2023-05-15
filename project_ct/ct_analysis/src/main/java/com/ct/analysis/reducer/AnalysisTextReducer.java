package com.ct.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据reducer
 */
public class AnalysisTextReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int sumCall = 0;    // 通话次数
        int sumDuration = 0;    // 通话时长

        for (Text value : values) {
            sumCall++;
            sumDuration += Integer.parseInt(value.toString());
        }

        context.write(key, new Text(sumCall + "_" + sumDuration));
    }
}
