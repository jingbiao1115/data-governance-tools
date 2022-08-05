package com.jb.table.enums;

/**
 * @author zhaojb
 * Interval枚举变量
 */
public enum IntervalEnum {
    SECOND(1,"second"),
    MINUTE(2,"minute")
    ;

    private int code;

    private String msg;

    IntervalEnum(int code,String msg) {
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
