package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdTradeOrderPaySucDetail
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:21
 * @description: DwdTradeOrderPaySucDetail
 */
public class DwdTradeOrderPaySucDetail {
    public static void main(String[] args) throws Exception {
        // 获取Flink流执行环境，用于后续配置和执行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置作业的并行度为4，即作业运行时将使用4个并行任务来处理数据
        env.setParallelism(4);
// 创建流表执行环境，用于在流处理中使用SQL语句进行数据操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次（EXACTLY_ONCE）的处理语义
// 确保在故障恢复时数据处理的一致性，不会出现数据重复或丢失的情况
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
// 注释掉的代码，若取消注释，会执行查询并打印topic_db表的所有数据，用于调试
 tableEnv.executeSql("select * from topic_db").print();

// 在流表环境中执行SQL语句，创建名为base_dic的表
// 该表用于从HBase读取数据，表结构包含dic_code（字典代码字符串）、info（包含dic_name的行类型）字段，dic_code为主键（但不强制约束）
// SQLUtil.getHBaseDDL方法用于获取创建HBase表的DDL语句，指定了HBase的表名
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
// 注释掉的代码，若取消注释，会执行查询并打印base_dic表的所有数据，用于调试
 tableEnv.executeSql("select * from base_dic").print();

// TODO 从下单事实表读取数据 创建动态表
// 在流表环境中执行SQL语句，创建名为dwd_trade_order_detail的表
// 该表用于从Kafka读取下单事实表数据，表结构包含多个字段，如id、order_id、user_id等
// SQLUtil.getKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了Kafka的主题和相关配置
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        " id string," +
                        " order_id string," +
                        " user_id string," +
                        " sku_id string," +
                        " sku_name string," +
                        " province_id string," +
                        " activity_id string," +
                        " activity_rule_id string," +
                        " coupon_id string," +
                        " date_id string," +
                        " create_time string," +
                        " sku_num string," +
                        " split_original_amount string," +
                        " split_activity_amount string," +
                        " split_coupon_amount string," +
                        " split_total_amount string," +
                        " ts_ms bigint " +
                        " )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

// TODO 过滤出支付成功数据
// 在topic_db表上执行SQL查询，筛选出source表名为'payment_info'、操作类型为'r'、支付状态不为空且支付状态为'1602'的数据
// 提取出相关字段，如user_id、order_id、payment_type等，结果存储在paymentInfo表中
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "ts_ms " +
                "from topic_db " +
                "where source['table'] ='payment_info' " +
                "and `op`='r' " +
                "and `after`['payment_status'] is not null " +
                "and `after`['payment_status'] = '1602' ");
// 将paymentInfo表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("payment_info", paymentInfo);
// 注释掉的代码，若取消注释，会执行查询并打印paymentInfo表的数据，用于调试
 paymentInfo.execute().print();

// TODO 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
// 在payment_info临时视图和dwd_trade_order_detail表上执行SQL查询，进行表连接操作
// 连接条件为payment_info表的order_id和dwd_trade_order_detail表的order_id相等
// 提取出连接后的相关字段，如order_detail_id、order_id、user_id等，结果存储在result表中
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts_ms " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id = od.order_id ");
// 注释掉的代码，若取消注释，会执行查询并打印result表的数据，用于调试
 result.execute().print();

// TODO 将关联的结果写到kafka主题中
// 在流表环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS的表
// 该表用于将处理后的数据写入Kafka，表结构包含多个字段，order_detail_id为主键（但不强制约束）
// SQLUtil.getUpsertKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了Kafka的主题和相关配置
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts_ms bigint ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

// 将result表中的数据插入到TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS表中，即将处理后的数据写入对应的Kafka主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

// 执行Flink作业，作业名称为DwdTradeOrderPaySucDetail
        env.execute("DwdTradeOrderPaySucDetail");
    }
}
