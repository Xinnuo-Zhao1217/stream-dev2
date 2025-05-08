package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdTradeOrderCancelDetail
 * @Author zhao.xinnuo
 * @Date 2025/5/4 20:45
 * @description: Flink 的流处理能力，从 Kafka 和 HBase 读取数据，筛选出订单取消相关的数据，经过处理后再将结果写回到 Kafka
 */
public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {

        // 获取Flink流执行环境，用于配置和执行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置作业并行度为4，即使用4个并行任务处理数据
        env.setParallelism(4);
// 创建流表执行环境，以便在流处理中使用SQL操作数据
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次语义
// 确保在故障恢复时数据处理的准确性，不会重复或丢失数据
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
// 设置空闲状态保留时间为30分钟零5秒，用于管理流处理中的状态
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

// 在流表环境中创建名为topic_db的表，用于从Kafka读取数据
// 表结构包含after（键值对映射）、source（键值对映射）、op（操作类型字符串）和ts_ms（时间戳长整型）字段
// SQLUtil.getKafkaDDL用于获取连接Kafka数据源的DDL语句，指定相关Kafka主题
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
// 注释掉的代码，若取消注释会查询并打印topic_db表所有数据，用于调试
 tableEnv.executeSql("select * from topic_db").print();

// 在流表环境中创建名为base_dic的表，用于从HBase读取数据
// 表结构包含dic_code（字典代码字符串）、info（包含dic_name的行类型）字段，dic_code设为主键（但不强制约束）
// SQLUtil.getHBaseDDL用于获取连接HBase数据源的DDL语句，指定HBase表名
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
// 注释掉的代码，若取消注释会查询并打印base_dic表所有数据，用于调试
 tableEnv.executeSql("select * from base_dic").print();

// 在topic_db表上执行SQL查询，筛选出与订单取消相关的数据
// 筛选条件为source表名为'order_info'，操作类型为'r'，且订单状态为'1001'或'1003'
// 提取出订单id、操作时间和时间戳等字段，结果存储在orderCancel表中
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +
                " `after`['operate_time'] as operate_time, " +
                " `ts_ms` " +
                " from topic_db " +
                " where source['table'] = 'order_info' " +
                " and `op` = 'r' " +
                " and `after`['order_status'] = '1001' " +
                " or `after`['order_status'] = '1003' ");
// 将orderCancel表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("order_cancel", orderCancel);
// 注释掉的代码，若取消注释会执行查询并打印orderCancel表数据，用于调试
 orderCancel.execute().print();

// 在流表环境中创建名为dwd_trade_order_detail的表，用于从Kafka读取下单事实表数据
// 表结构包含订单相关的众多字段，如id、order_id、user_id等
// SQLUtil.getKafkaDDL用于获取连接Kafka数据源的DDL语句，指定相关Kafka主题
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "  id string," +
                        "  order_id string," +
                        "  user_id string," +
                        "  sku_id string," +
                        "  sku_name string," +
                        "  province_id string," +
                        "  activity_id string," +
                        "  activity_rule_id string," +
                        "  coupon_id string," +
                        "  date_id string," +
                        "  create_time string," +
                        "  sku_num string," +
                        "  split_original_amount string," +
                        "  split_activity_amount string," +
                        "  split_coupon_amount string," +
                        "  split_total_amount string," +
                        "  ts_ms bigint " +
                        "  )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

// 在dwd_trade_order_detail表和order_cancel临时视图上执行SQL查询，进行连接操作
// 连接条件为订单id相等，提取出连接后相关字段，结果存储在result表中
// 对create_time字段进行转换处理，生成date_id字段
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "  od.id," +
                        "  od.order_id," +
                        "  od.user_id," +
                        "  od.sku_id," +
                        "  od.sku_name," +
                        "  od.province_id," +
                        "  od.activity_id," +
                        "  od.activity_rule_id," +
                        "  od.coupon_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  oc.operate_time," +
                        "  od.sku_num," +
                        "  od.split_original_amount," +
                        "  od.split_activity_amount," +
                        "  od.split_coupon_amount," +
                        "  od.split_total_amount," +
                        "  oc.ts_ms " +
                        "  from dwd_trade_order_detail od " +
                        "  join order_cancel oc " +
                        "  on od.order_id = oc.id ");
// 注释掉的代码，若取消注释会执行查询并打印result表数据，用于调试
 result.execute().print();

// 在流表环境中创建名为TOPIC_DWD_TRADE_ORDER_CANCEL的表，用于将处理后的数据写入Kafka
// 表结构包含众多订单相关字段，id设为主键（但不强制约束）
// SQLUtil.getUpsertKafkaDDL用于获取将数据写入Kafka的DDL语句
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                        "  id string," +
                        "  order_id string," +
                        "  user_id string," +
                        "  sku_id string," +
                        "  sku_name string," +
                        "  province_id string," +
                        "  activity_id string," +
                        "  activity_rule_id string," +
                        "  coupon_id string," +
                        "  date_id string," +
                        "  cancel_time string," +
                        "  sku_num string," +
                        "  split_original_amount string," +
                        "  split_activity_amount string," +
                        "  split_coupon_amount string," +
                        "  split_total_amount string," +
                        "  ts_ms bigint ," +
                        "  PRIMARY KEY (id) NOT ENFORCED " +
                        "  )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
// 将result表中的数据插入到TOPIC_DWD_TRADE_ORDER_CANCEL表，即写入对应的Kafka主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

// 执行Flink作业，作业名称为DwdTradeOrderCancelDetail
        env.execute("DwdTradeOrderCancelDetail");
    }
}
