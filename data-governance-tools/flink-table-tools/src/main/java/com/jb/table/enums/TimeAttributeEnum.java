package com.jb.table.enums;

/**
 * @author zhaojb
 * FlinkTable时间属性
 */
public enum TimeAttributeEnum {

    EVENT_TIME(1,"事件时间属性"),
    PROCESSING_TIME(2,"处理时间属性")
    ;

    private int code;

    private String msg;

    TimeAttributeEnum(int code,String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
