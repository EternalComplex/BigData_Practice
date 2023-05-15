package com.ct.common.bean;

/**
 * 数据对象
 */
public abstract class Data implements Val {

    public String content;


    public void setValue(Object value) {
        this.content = (String) value;
    }

    public String getValue() {
        return this.content;
    }
}
