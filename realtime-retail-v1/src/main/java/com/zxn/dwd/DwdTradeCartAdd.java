package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zxn.dwd.DwdTradeCartAdd
 * @Author zhao.xinnuo
 * @Date 2025/5/4 20:44
 * @description: DwdTradeCartAdd
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // 获取Flink的流执行环境，用于配置和运行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置作业的并行度为4，意味着Flink会以4个并行任务来处理数据
        env.setParallelism(4);

// 创建流表执行环境，它允许在流处理环境中使用SQL语句来操作数据
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 启用检查点机制，设置检查点间隔为5000毫秒（即5秒），采用精确一次（Exactly Once）的处理语义
// 这能保证在发生故障时，数据处理不会出现重复或丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

// 在表执行环境中执行SQL语句，创建名为topic_db的表
// 此表用于从Kafka读取数据，表结构包含after（键值对映射）、source（键值对映射）、op（操作类型字符串）和ts_ms（时间戳长整型）字段
// SQLUtil.getKafkaDDL方法用于生成连接Kafka所需的DDL语句，这里指定了Kafka主题和相关配置
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

// 注释掉的代码，若取消注释，会执行查询并打印topic_db表的所有数据，用于调试
 tableEnv.executeSql("select * from topic_db").print();

// 在表执行环境中执行SQL语句，创建名为base_dic的表
// 此表用于从HBase读取数据，表结构包含dic_code（字典代码字符串）、info（包含dic_name的行类型）字段，dic_code作为主键
// SQLUtil.getHBaseDDL方法用于生成连接HBase所需的DDL语句，指定了HBase表名
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );

// 注释掉的代码，若取消注释，会执行查询并打印base_dic表的所有数据，用于调试
 tableEnv.executeSql("select * from base_dic").print();

// 在表执行环境中执行SQL查询，从topic_db表筛选出与购物车信息相关的数据
// 筛选条件为source表名为'cart_info'，操作类型为'r'或者满足特定的sku_num条件
// 对于筛选出的数据，提取id、user_id、sku_id等字段，其中sku_num字段根据操作类型进行不同处理
// 若操作类型为'r'，直接取after['sku_num']；否则计算差值
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op = 'r',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`after`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts_ms \n" +
                "   from topic_db \n" +
                "   where source['table'] = 'cart_info' \n" +
                "   and ( op = 'r' or \n" +
                "   ( op='r' and after['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(after['sku_num'] AS INT))))"
        );

// 注释掉的代码，若取消注释，会执行查询并打印cartInfo表的数据，用于调试
 cartInfo.execute().print();
// 在表执行环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_CART_ADD的表
// 此表用于将处理后的数据写入Kafka，表结构包含id、user_id、sku_id、sku_num和ts_ms字段，id作为主键
// SQLUtil.getUpsertKafkaDDL方法用于生成将数据写入Kafka所需的DDL语句
        tableEnv.executeSql(" create table "+ Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

// 将cartInfo表中的数据插入到TOPIC_DWD_TRADE_CART_ADD表，也就是将处理后的数据写入对应的Kafka主题
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

// 执行Flink作业，作业名称为'dwd_kafka'
        env.execute("dwd_kafka");
    }
}
