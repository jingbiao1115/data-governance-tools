package com.jb.table.enity.connector;

import com.jb.table.enity.connector.attributes.TimeAttributes;
import com.jb.table.enity.connector.with.JdbcConnectorWith;
import lombok.Data;
import lombok.Getter;

import javax.validation.constraints.NotBlank;
import java.util.List;

/**
 * @author zhaojb
 * JDBC Connector 对象
 */
@Getter
public class JdbcConnector {

    /**
     * Flink表名
     */
    private String flinkCreateTableName;

    /**
     * 数据库表建表语句
     */
    @NotBlank(message = "createTable is not null")
    private String createTable;

    /**
     * 指定的Flink Table主键
     */
    private List<String> primaryKeys;

    /**
     * Flink时间属性
     */
    private String  timeAttributes;

    /**
     * Connector WITH
     */
    @NotBlank(message = "WITH is not null")
    private JdbcConnectorWith with;

    public static JdbcConnector create(String flinkCreateTableName,
                                       String createTable,
                                       JdbcConnectorWith with) {

        return new JdbcConnector(flinkCreateTableName,createTable,with);
    }

    private JdbcConnector(String flinkCreateTableName,
                         String createTable,
                         JdbcConnectorWith with) {

        this.flinkCreateTableName = flinkCreateTableName;
        this.createTable = createTable;
        this.with = with;
    }

    /**
     * FlinkTable建表主键
     */
    public JdbcConnector withPrimaryKeys(List<String> primaryKeys){
        this.primaryKeys = primaryKeys;

        return this;
    }

    /**
     * FlinkTable建表时间属性,将List转换成可用的String
     */
    public JdbcConnector withTimeAttributes(List<TimeAttributes> attributes){

        StringBuilder timeAttributeBuilder= new StringBuilder();

        for(TimeAttributes timeAttribute:attributes){
            timeAttributeBuilder.append(timeAttribute.getTimeAttribute());
        }

        this.timeAttributes = timeAttributeBuilder.toString();

        return this;
    }

}
