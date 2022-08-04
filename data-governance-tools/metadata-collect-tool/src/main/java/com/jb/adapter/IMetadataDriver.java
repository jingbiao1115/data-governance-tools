package com.jb.adapter;

/**
 * @author zhaojb
 * 数据源适配,包括关系型数据库,消息队列,大数据,ES
 */
public interface IMetadataDriver {
    /**
     * 创建连接
     */
    void createConnection() throws Exception;

    /**
     * 关闭连接
     */
    void close() ;
}
