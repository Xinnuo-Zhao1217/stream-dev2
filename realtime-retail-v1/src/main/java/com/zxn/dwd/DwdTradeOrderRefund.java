package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdTradeOrderRefund
 * @Author zhao.xinnuo
 * @Date 2025/5/4 22:57
 * @description: 用于处理电商交易中的退单数据
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {
        // 获取Flink的流执行环境，后续用于配置和执行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置作业的并行度为4，即作业会以4个并行任务来处理数据
        env.setParallelism(4);
// 创建流表执行环境，以便能在流处理中使用SQL语句进行数据操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 启用检查点机制，每5000毫秒（也就是5秒）进行一次检查点操作，采用精确一次（EXACTLY_ONCE）的处理语义
// 这样能保证在发生故障时，数据处理不会出现重复或丢失的情况
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
// 设置空闲状态的保留时间为30分钟零5秒，用于管理流处理过程中的状态数据
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

// 在流表环境中执行SQL语句，创建名为topic_db的表
// 该表用于从Kafka读取数据，表结构包含after（键值对映射）、source（键值对映射）、op（操作类型字符串）和ts_ms（时间戳长整型）字段
// SQLUtil.getKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了Kafka的主题和相关配置
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
// 注释掉的代码，若取消注释会执行查询并打印topic_db表的所有数据，用于调试
// tableEnv.executeSql("select * from topic_db").print();

// 在流表环境中执行SQL语句，创建名为base_dic的表
// 该表用于从HBase读取数据，表结构包含dic_code（字典代码字符串）、info（包含dic_name的行类型）字段，dic_code为主键（但不强制约束）
// SQLUtil.getHBaseDDL方法用于获取创建HBase表的DDL语句，指定了HBase的表名
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
// 注释掉的代码，若取消注释会执行查询并打印base_dic表的所有数据，用于调试
// tableEnv.executeSql("select * from base_dic").print();

// 2. 过滤退单表数据 order_refund_info   insert
// 在topic_db表上执行SQL查询，筛选出source表名为'order_refund_info'且操作类型为'r'的数据
// 提取出相关字段，如id、user_id、order_id等，结果存储在orderRefundInfo表中
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['user_id'] user_id," +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['refund_type'] refund_type," +
                        " after['refund_num'] refund_num," +
                        " after['refund_amount'] refund_amount," +
                        " after['refund_reason_type'] refund_reason_type," +
                        " after['refund_reason_txt'] refund_reason_txt," +
                        " after['create_time'] create_time," +
                        " ts_ms " +
                        " from topic_db " +
                        " where source['table'] = 'order_refund_info' " +
                        " and `op`='r' ");
// 将orderRefundInfo表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
// 注释掉的代码，若取消注释会执行查询并打印orderRefundInfo表的数据，用于调试
 orderRefundInfo.execute().print();

// 3. 过滤订单表中的退单数据: order_info  update
// 在topic_db表上执行SQL查询，筛选出source表名为'order_info'、操作类型为'r'、订单状态不为空且订单状态为'1006'的数据
// 提取出相关字段，如id、province_id，结果存储在orderInfo表中
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['province_id'] province_id " +
                        " from topic_db " +
                        " where source['table'] = 'order_info' " +
                        " and `op`='r' " +
                        " and `after`['order_status'] is not null " +
                        " and `after`['order_status'] = '1006' ");
// 将orderInfo表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_info", orderInfo);
// 注释掉的代码，若取消注释会执行查询并打印orderInfo表的数据，用于调试
// orderInfo.execute().print();

// 4. join: 普通的和 lookup join
// 在order_refund_info临时视图和order_info临时视图上执行SQL查询，进行表连接操作
// 连接条件为order_refund_info表的order_id和order_info表的id相等
// 提取出连接后的相关字段，如id、user_id、order_id等，并对create_time字段进行转换处理，生成date_id字段，结果存储在result表中
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  ri.id," +
                        "  ri.user_id," +
                        "  ri.order_id," +
                        "  ri.sku_id," +
                        "  oi.province_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  ri.create_time," +
                        "  ri.refund_type," +
                        "  ri.refund_reason_type," +
                        "  ri.refund_reason_txt," +
                        "  ri.refund_num," +
                        "  ri.refund_amount," +
                        "  ri.ts_ms " +
                        "  from order_refund_info ri " +
                        "  join order_info oi " +
                        "  on ri.order_id=oi.id ");
// 注释掉的代码，若取消注释会执行查询并打印result表的数据，用于调试
// result.execute().print();

// 5. 写出到 kafka
// 在流表环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_ORDER_REFUND的表
// 该表用于将处理后的数据写入Kafka，表结构包含多个字段，id为主键（但不强制约束）
// SQLUtil.getUpsertKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了Kafka的主题和相关配置
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_reason_type_code string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

// 将result表中的数据插入到TOPIC_DWD_TRADE_ORDER_REFUND表中，即将处理后的数据写入对应的Kafka主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

// 执行Flink作业，作业名称为DwdTradeOrderRefund
        env.execute("DwdTradeOrderRefund");
    }
}
