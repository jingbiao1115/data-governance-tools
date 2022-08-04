package com.jb.table.env;


/**
 * @author zhaojb
 */
public class FlinkTableEnv {

    private FlinkTableEnv() {
    }

//    public static TableEnvironment builderEnv() {
////        EnvironmentSettings settings = EnvironmentSettings.newInstance()
////                .useBlinkPlanner()
////                .inBatchMode()
////                .build();
//
//        //Flink  1.15.0
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//
//        TableEnvironment tEnv = TableEnvironment.create(settings);
//
//        return tEnv;
//    }
//
//    public static Table builderTable(TableEnvironment env,SchemaTypeEnum schemaType,IParam param,
//            String tableName,String createTableInfo,InspectRangeEnum inspectRange,
//            IncrementValue incrementValue) {
//        ISourceSchema sourceSchema = null;
//
//        switch (schemaType.getCode()) {
//            case 0:
//                sourceSchema = new MysqlSourceDriver();
//                break;
//
//            default:
//                throw new GlobalException("SchemaTypeEnum参数异常");
//        }
//
//        env.executeSql(sourceSchema.createConnection(param,tableName,createTableInfo));
//
//        return env.sqlQuery("select * from " + tableName);
//    }

}
