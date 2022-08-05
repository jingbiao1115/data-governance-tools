package com.jb.table.enity.connector.with;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author zhaojb
 * Kafka Connector WITH对象
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class KafkaConnectorWith extends ConnectorWith{

    /**
     * kafka topic
     */
    private String topic;

    /**
     * Kafka scan.startup.mode
     */
    private String scanStartupMode;

    /**
     * Kafka properties.bootstrap.servers
     */
    private String propertiesBootstrapServers;

    /**
     * Kafka format,Flink 1.15中format:json过期
     */
    private String format;


}
