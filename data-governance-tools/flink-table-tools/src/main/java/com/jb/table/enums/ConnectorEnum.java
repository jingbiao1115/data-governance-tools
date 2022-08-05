package com.jb.table.enums;

/**
 * @author zhaojb
 * Flink Table Connector枚举
 */
public enum ConnectorEnum {

    JDBC(1,"jdbc"),
    KAFKA(2,"kafka");

    private int code;

    private String msg;

    ConnectorEnum(int code,String msg) {
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
