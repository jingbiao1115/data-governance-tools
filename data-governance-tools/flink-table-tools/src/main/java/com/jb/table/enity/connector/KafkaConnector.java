package com.jb.table.enity.connector;

import com.jb.table.enity.connector.with.KafkaConnectorWith;
import lombok.Data;

/**
 * @author zhaojb
 * KAFKA Connector 对象
 */
@Data
public class KafkaConnector {

    /**
     * 建表语句
     */
    private String createTable;

    /**
     * Connector WITH
     */
    private KafkaConnectorWith with;

    public KafkaConnector(String createTable,KafkaConnectorWith with) {
        this.createTable = createTable;
        this.with = with;
    }
}
