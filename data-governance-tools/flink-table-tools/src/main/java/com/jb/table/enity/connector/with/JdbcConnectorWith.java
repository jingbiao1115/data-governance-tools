package com.jb.table.enity.connector.with;

import com.jb.table.enity.IParameter;
import com.jb.table.enity.parameter.MysqlParameter;
import com.jb.table.enums.ConnectorEnum;
import com.jb.table.enums.FlinkTableDataSourceEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.NotBlank;

/**
 * @author zhaojb
 * JDBC Connector WITH对象
 */
@Getter
public class JdbcConnectorWith extends ConnectorWith {

    @NotBlank(message = "dataSourceType is not null")
    private FlinkTableDataSourceEnum dataSourceType;

    /**
     * JDBC url:jdbc:mysql://localhost:3306/mydatabase
     */
    @NotBlank(message = "jdbcUrl is not null")
    private String jdbcUrl;

    /**
     * JDBC table-name
     */
    @NotBlank(message = "tableName is not null")
    private String tableName;

    /**
     * jdbc驱动
     */
    @NotBlank(message = "driver is not null")
    private String driver;

    /**
     * jdbc 连接账号
     */
    @NotBlank(message = "username is not null")
    private String username;

    /**
     * jdbc 连接密码
     */
    @NotBlank(message = "password is not null")
    private String password;

    /**
     * jdbc 最大重试超时(单位:s),默认60s
     * connection.max-retry-timeout
     */
    private String connectionMaxRetryTimeout = "60s";

    /**
     * jdbc 是否在事务中自动提交,默认true
     * scan.auto-commit
     */
    private String scanAutoCommit = "true";

    /**
     * jdbc sink 并行度,默认情况下，并行性由框架使用与上游链式操作符相同的并行性来确定
     * sink.parallelism
     */
    private String sinkParallelism;

    /**
     * jdbc 查找数据库失败时,最大重试次数,默认3
     * lookup.max-retries
     */
    private String lookupMaxRetries = "3";

    public static JdbcConnectorWith config(FlinkTableDataSourceEnum dataSourceType,
                                           IParameter parameter,
                                           String tableName) {
        return new JdbcConnectorWith(dataSourceType,parameter,tableName);
    }

    private JdbcConnectorWith(FlinkTableDataSourceEnum dataSourceType,IParameter parameter,
                             String tableName) {
        this.dataSourceType = dataSourceType;
        this.tableName = tableName;

        switch (dataSourceType.getCode()) {
            case 0://
                MysqlParameter mysqlParameter = (MysqlParameter)parameter;

                this.setConnector(ConnectorEnum.JDBC);
                this.driver = "com.jdbc.mysql";
                this.username = mysqlParameter.getUsername();
                this.password = mysqlParameter.getPassword();

                this.jdbcUrl = "jdbc:mysql://" +
                        mysqlParameter.getIp() +
                        ":" +
                        mysqlParameter.getPort() +
                        "/" +
                        mysqlParameter.getCatalog();

                break;
            default:
                throw new RuntimeException("dataSourceType is  mismatch");
        }
    }

    public JdbcConnectorWith withConnectionMaxRetryTimeout(String connectionMaxRetryTimeout) {
        this.connectionMaxRetryTimeout = connectionMaxRetryTimeout;

        return this;
    }


    public JdbcConnectorWith withScanAutoCommit(String scanAutoCommit) {
        this.scanAutoCommit = scanAutoCommit;

        return this;
    }

    public JdbcConnectorWith withSinkParallelism(String sinkParallelism) {
        this.sinkParallelism = sinkParallelism;

        return this;
    }

    public JdbcConnectorWith withLookupMaxRetries(String lookupMaxRetries) {
        this.lookupMaxRetries = lookupMaxRetries;

        return this;
    }


}
