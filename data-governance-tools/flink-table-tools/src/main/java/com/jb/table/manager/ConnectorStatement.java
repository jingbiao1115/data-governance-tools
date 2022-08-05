package com.jb.table.manager;

import cn.hutool.core.util.ObjectUtil;
import com.jb.table.enity.connector.JdbcConnector;
import com.jb.table.enity.connector.with.JdbcConnectorWith;
import com.jb.table.enums.FlinkTableDataSourceEnum;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @author zhaojb
 * Flink Table Connector语句转换及构建
 */
public class ConnectorStatement {


    public static String jdbcConnectorStatement(JdbcConnector jdbcConnector) {

        FlinkTableDataSourceEnum dataSourceType = jdbcConnector.getWith().getDataSourceType();

        try {
            switch (dataSourceType.getCode()) {
                case 0:
                    //mysql
                    return toFlinkCreateTableWithSql(jdbcConnector);

                default:
                    throw new RuntimeException("dataSourceType is mismatch");
            }
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 将数据库表建表语句转为FlinkTable建表语句+With参数
     */
    public static String toFlinkCreateTableWithSql(JdbcConnector jdbcConnector) throws SqlParseException {

        String flinkCreateTableName = jdbcConnector.getFlinkCreateTableName();
        String tableName = jdbcConnector.getWith().getTableName();
        JdbcConnectorWith jdbcConnectorWith = jdbcConnector.getWith();

        //flinkCreateTableName为空时,其默认值为tableName
        if (ObjectUtil.isNull(flinkCreateTableName)) {
            flinkCreateTableName = tableName;

        }

        return toFlinkCreateTableBaseSql(flinkCreateTableName,
                                         jdbcConnector.getCreateTable(),
                                         jdbcConnector.getPrimaryKeys(),
                                         jdbcConnector.getTimeAttributes()) +

                " WITH (" +
                "'connector' = '" + jdbcConnectorWith.getConnector().name() + "'," +
                "'url' = '" + jdbcConnectorWith.getJdbcUrl() +
                "'," +
                "'table-name' = '" + jdbcConnectorWith.getTableName() + "'," +
                "'driver' = '" + jdbcConnectorWith.getDriver() + "'," +
                "'username' = '" + jdbcConnectorWith.getUsername() + "'," +
                "'password' = '" + jdbcConnectorWith.getPassword() + "'," +
                "'connection.max-retry-timeout' = '" + jdbcConnectorWith.getConnectionMaxRetryTimeout() + "'," +
                "'scan.auto-commit' = '" + jdbcConnectorWith.getScanAutoCommit() + "'," +
                "'sink.parallelism' = '" + jdbcConnectorWith.getSinkParallelism() + "'," +
                "'lookup.max-retries' = '" + jdbcConnectorWith.getLookupMaxRetries() + '\'' +
                ')';

    }

    /**
     * 将数据库字段类型转换为Flink Table字段类型,并将其构建为FlinkTable建表Sql语句
     *
     * @param flinkCreateTableName FlinkTable
     * @param createTable          真实的建表语句
     */
    public static String toFlinkCreateTableBaseSql(String flinkCreateTableName,
                                                   String createTable,List<String> primaryKeys,
                                                   String timeAttributes) throws SqlParseException {

        List<SqlNode> sqlNodes =
                ((SqlCreateTable)createTableSqlParse(createTable).parseStmt()).columnList.getList();

        StringBuilder createTableColumnBuilder = new StringBuilder();

        for (SqlNode sqlNode: sqlNodes) {
            String column = ((SqlColumnDeclaration)sqlNode).name.getSimple();
            String type = ((SqlColumnDeclaration)sqlNode).dataType.getTypeName().getSimple();

            createTableColumnBuilder
                    .append(column)
                    .append(' ')
                    //此处字段类型需做类型转换
                    .append(type)
                    .append(',');
        }

        createTableColumnBuilder.delete(createTableColumnBuilder.length() - 1,
                                        createTableColumnBuilder.length());


        //FlinkTable主键
        String flinkTablePrimaryKeys = null;
        if (ObjectUtil.isNotNull(primaryKeys)) {

            createTableColumnBuilder.append(',');

            flinkTablePrimaryKeys = "PRIMARY KEY (" +
                    primaryKeys.stream().collect(Collectors.joining(",")) +
                    ")";
        }


        return "create table " +
                flinkCreateTableName +
                '(' +
                createTableColumnBuilder +
                flinkTablePrimaryKeys +

                //FlinkTable时间属性
                timeAttributes +

                ')';
    }

    /**
     * SqlParse工厂create
     */
    public static SqlParser createTableSqlParse(String sql) {

        return SqlParser.create(sql,createFactorySqlParser(SqlDdlParserImpl.FACTORY));
    }

    /**
     * SqlParse工厂select
     */
    public static SqlParser selectTableSqlParse(String sql) {

        return SqlParser.create(sql,createFactorySqlParser(SqlParserImpl.FACTORY));
    }

    /**
     * 创建SqlParse工厂
     * sqlParserImplFactory指定ddl或dml
     * ddl:SqlDdlParserImpl.FACTORY
     */
    public static SqlParser.Config createFactorySqlParser(SqlParserImplFactory sqlParserImplFactory) {

        return SqlParser
                .config()
                //设置引用一个标识符 默认为空
                .withQuoting(Quoting.BACK_TICK)
                //当字段未被标识时大小写策略
                .withQuotedCasing(Casing.TO_LOWER)
                //当字段被标识时大小写策略
                .withUnquotedCasing(Casing.TO_LOWER)
                // 设置字段的最大长度，超出会抛错 128
                .withIdentifierMaxLength(128)
                //是否区分大小写
                .withCaseSensitive(false)
                .withParserFactory(sqlParserImplFactory);
    }

}
