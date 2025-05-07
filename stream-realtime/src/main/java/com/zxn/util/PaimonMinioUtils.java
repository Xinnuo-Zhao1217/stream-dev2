package com.zxn.util;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zxn.util.PaimonMinioUtils
 * @Author zhao.xinnuo
 * @Date 2025/5/7 13:45
 * @description: PaimonMinioUtils
 */
public class PaimonMinioUtils {
    public static void ExecCreateMinioCatalogAndDatabases(StreamTableEnvironment tenv, String catalogName, String databaseName){
        System.setProperty("HADOOP_USER_NAME","root");
        if (catalogName.length() == 0){
            catalogName = "minio_paimon_catalog";
        }
        tenv.executeSql("CREATE CATALOG "+ catalogName +"                             " +
                "WITH                                                                   " +
                "  (                                                                    " +
                "    'type' = 'paimon',                                                 " +
                "    'warehouse' = 's3://paimon-data/',                                 " +
                "    's3.endpoint' = 'http://10.160.60.17:9000',                         " +
                "    's3.access-key' = 'X7pljEi3steavVn5h3z3',                          " +
                "    's3.secret-key' = 'KDaSxEyfSEmKiaJDBbJ6RpBxMBp6OwnRbkA8LnKL',      " +
                "    's3.connection.ssl.enabled' = 'false',                             " +
                "    's3.path.style.access' = 'true',                                   " +
                "    's3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',              " +
                "    's3.aws.credentials.provider' = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' " +
                "  );");
        tenv.executeSql("use catalog "+catalogName+";");
        tenv.executeSql("create database if not exists "+databaseName+";");
    }
}
