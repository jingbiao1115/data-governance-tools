package com.jb.table.enity.connector.with;

import com.jb.table.enums.ConnectorEnum;
import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * @author zhaojb
 * Connector WITH对象
 */
@Data
public class ConnectorWith {
    /**
     * Connector:JDBC,Kafka
     */
    @NotBlank(message = "connector is not null")
    protected ConnectorEnum connector;

}
