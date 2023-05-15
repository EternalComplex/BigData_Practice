package com.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {
    // 设置数据来源
    public void setIn(DataIn in);

    // 设置数据去向
    public void setOut(DataOut out);

    // 生产数据
    public void produce();
}
