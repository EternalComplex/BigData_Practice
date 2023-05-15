package com.ct.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据value
 */
public class AnalysisValue implements Writable {
    private Integer sumCall;

    private Integer sumDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(Integer sumCall, Integer sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.sumCall);
        dataOutput.writeInt(this.sumDuration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sumCall = dataInput.readInt();
        this.sumDuration = dataInput.readInt();
    }

    public Integer getSumCall() {
        return sumCall;
    }

    public void setSumCall(Integer sumCall) {
        this.sumCall = sumCall;
    }

    public Integer getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Integer sumDuration) {
        this.sumDuration = sumDuration;
    }
}
