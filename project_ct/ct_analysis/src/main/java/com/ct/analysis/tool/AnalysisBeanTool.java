package com.ct.analysis.tool;

import com.ct.analysis.io.MySqlBeanOutputFormat;
import com.ct.analysis.io.MySqlTextOutputFormat;
import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import com.ct.analysis.mapper.AnalysisBeanMapper;
import com.ct.analysis.mapper.AnalysisTextMapper;
import com.ct.analysis.reducer.AnalysisBeanReducer;
import com.ct.analysis.reducer.AnalysisTextReducer;
import com.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * 分析数据的工具类
 */
public class AnalysisBeanTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf(Bytes.toBytes(Names.NAMESPACE.getValue()), Bytes.toBytes(Names.TABLE.getValue())),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );

        // reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        // outputFormat
        job.setOutputFormatClass(MySqlBeanOutputFormat.class);

        boolean flag = job.waitForCompletion(true);
        if (flag) {
            return JobStatus.SUCCEEDED;
        } else {
            return JobStatus.FAILED;
        }
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
