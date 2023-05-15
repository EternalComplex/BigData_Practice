package com.ct.analysis.tool;

import com.ct.analysis.io.MySqlTextOutputFormat;
import com.ct.analysis.mapper.AnalysisTextMapper;
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
public class AnalysisTextTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf(Bytes.toBytes(Names.NAMESPACE.getValue()), Bytes.toBytes(Names.TABLE.getValue())),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );

        // reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // outputFormat
        job.setOutputFormatClass(MySqlTextOutputFormat.class);

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
