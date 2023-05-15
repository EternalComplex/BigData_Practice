package com.ct.common.constant;

import com.ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    NAMESPACE("ct"),        // 命名空间名称
    TABLE("calllog"),       // 表名
    CF_CALLER("caller"),    // 列族名称: 主叫号码
    CF_CALLEE("callee"),    // 列族名称: 被叫号码
    CF_INFO("info"),        // 列族名称
    REGION_COUNT("6"),      // 分区数
    TOPIC("ct");

    private String name;

    private Names(String name) {
        this.name = name;
    }

    public void setValue(Object value) {
        this.name = (String) value;
    }

    public String getValue() {
        return this.name;
    }
}
