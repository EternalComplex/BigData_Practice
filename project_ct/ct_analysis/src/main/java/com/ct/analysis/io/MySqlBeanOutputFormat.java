package com.ct.analysis.io;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import com.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySql格式化输出对象
 */
public class MySqlBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    protected static class MySqlRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {
        private Connection conn = null;
        private Jedis jedis = null;

        public MySqlRecordWriter() {
            // 获取资源
            conn = JDBCUtil.getConnection();
            jedis = new Jedis("192.168.10.199", 6379);
            jedis.auth("redis");
        }

        // 输出数据
        @Override
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {
            PreparedStatement pstat = null;
            try {
                String insertSql = "insert into ct_call(tel_id, date_id, sum_call, sum_duration) values(?, ?, ?, ?)";
                pstat = conn.prepareStatement(insertSql);
                pstat.setInt(1, Integer.parseInt(jedis.hget("ct_user", key.getTel())));
                pstat.setInt(2, Integer.parseInt(jedis.hget("ct_date", key.getDate())));
                pstat.setInt(3, value.getSumCall());
                pstat.setInt(4, value.getSumDuration());
                pstat.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (pstat != null) {
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        // 释放资源
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    // getOutputCommitter虽然用不到，但是不能直接返回null，仿照其它实现类编写。防止空指针异常。
    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}
