package com.jb.table.manager;

import com.jb.table.enity.connector.JdbcConnector;
import com.jb.table.enity.connector.attributes.TimeAttributes;
import com.jb.table.enity.connector.with.JdbcConnectorWith;
import com.jb.table.enity.parameter.MysqlParameter;
import com.jb.table.enums.FlinkTableDataSourceEnum;
import com.jb.table.enums.IntervalEnum;

import java.util.Arrays;

/**
 * @author zhaojb
 * <p>
 * 构建Connector
 */
public class ConnectorBuilder {


    public static void main(String[] args) {


        MysqlParameter parameter = new MysqlParameter();
        parameter.setCatalog("d1");
        parameter.setIp("10.10.14.108");
        parameter.setPort(3306);
        parameter.setUsername("root");
        parameter.setPassword("123456");

        JdbcConnectorWith jdbcConnectorWith =
                JdbcConnectorWith.config(FlinkTableDataSourceEnum.MYSQL,
                                         parameter,"t1");

        JdbcConnector jdbcConnector = JdbcConnector.create(
                        "flink_table",
                        "create table t1(id int ,name varchar(255),dt datetime)",
                        jdbcConnectorWith)
                .withPrimaryKeys(Arrays.asList("id","name"))
                .withTimeAttributes(Arrays.asList(
                        TimeAttributes.eventTimeBuilder("col1",5,IntervalEnum.SECOND),
                        TimeAttributes.processingTimeBuilder("col2"))
                );


        System.out.println(ConnectorStatement.jdbcConnectorStatement(jdbcConnector));


    }


}
