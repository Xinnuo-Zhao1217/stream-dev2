package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zxn.dwd.DwdInteractionCommentInfo
 * @Author zhao.xinnuo
 * @Date 2025/5/4 20:43
 * @description: 实现了从 Kafka 读取数据，结合 HBase 中的字典数据进行处理，最终将处理结果写回 Kafka 的流处理过程。
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境，用于配置和执行Flink流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置作业的并行度为4，即Flink将使用4个并行任务来处理数据
        env.setParallelism(4);

// 创建流表执行环境，用于在流处理环境中执行SQL查询和操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 启用检查点机制，设置检查点间隔为5000毫秒（即5秒），检查点模式为精确一次
// 精确一次模式确保数据在故障恢复时不会丢失或重复处理
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

// 注释掉的代码，原本用于设置重启策略，这里表示在30天内允许最多3次失败，每次失败后等待3秒再重启
// env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

// 在表执行环境中执行SQL语句，创建一个名为topic_db的表
// 该表从Kafka主题中读取数据，包含after、source、op和ts_ms四个字段
// SQLUtil.getKafkaDDL方法用于获取Kafka数据源的DDL语句
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  `ts_ms` bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

// 注释掉的代码，原本用于查询topic_db表的所有数据并打印结果
 tableEnv.executeSql("select * from topic_db").print();

// 在topic_db表上执行SQL查询，筛选出source表名为comment_info且操作类型为'r'的记录
// 并从after字段中提取所需的列，如id、user_id等，将结果存储在commentInfo表中
        Table commentInfo = tableEnv.sqlQuery("select  \n" +
                "    `after`['id'] as id, \n" +
                "    `after`['user_id'] as user_id, \n" +
                "    `after`['sku_id'] as sku_id, \n" +
                "    `after`['appraise'] as appraise, \n" +
                "    `after`['comment_txt'] as comment_txt, \n" +
                "    `after`['create_time'] as create_time, " +
                "     ts_ms " +
                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");

// 注释掉的代码，原本用于执行commentInfo表的查询并打印结果
// commentInfo.execute().print();

// 将commentInfo表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("comment_info", commentInfo);

// 在表执行环境中执行SQL语句，创建一个名为base_dic的表
// 该表从HBase中读取数据，包含dic_code和info字段，dic_code作为主键
// SQLUtil.getHBaseDDL方法用于获取HBase数据源的DDL语句
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));

// 注释掉的代码，原本用于查询base_dic表的所有数据并打印结果
// tableEnv.executeSql("select * from base_dic").print();

// 在comment_info表和base_dic表上执行SQL查询，进行连接操作
// 将comment_info表中的appraise字段与base_dic表中的dic_code字段进行匹配
// 并将匹配结果中的dic_name字段重命名为appraise_name，将结果存储在joinedTable中
        Table joinedTable = tableEnv.sqlQuery("SELECT  \n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts_ms \n" +
                "    FROM comment_info AS c\n" +
                "    JOIN base_dic AS dic\n" +
                "    ON c.appraise = dic.dic_code");

// 注释掉的代码，原本用于执行joinedTable表的查询并打印结果
// joinedTable.execute().print();

// 在表执行环境中执行SQL语句，创建一个名为TOPIC_DWD_INTERACTION_COMMENT_INFO的表
// 该表用于将数据写入Kafka主题，包含id、user_id等字段，id作为主键
// SQLUtil.getUpsertKafkaDDL方法用于获取Kafka数据写入的DDL语句
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED \n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

// 将joinedTable表中的数据插入到TOPIC_DWD_INTERACTION_COMMENT_INFO表中，即写入Kafka主题
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


    }
}
