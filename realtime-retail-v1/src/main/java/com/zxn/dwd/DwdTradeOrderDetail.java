package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdTradeOrderDetail
 * @Author zhao.xinnuo
 * @Date 2025/5/4 20:46
 * @description: DwdTradeOrderDetail
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        // 获取Flink的流执行环境，用于后续配置和执行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置作业的并行度为4，即作业将使用4个并行任务来处理数据
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

// TODO 过滤出订单明细数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_detail'且操作类型为'r'的数据
// 提取出相关字段，如id、order_id、sku_id等，并对某些字段进行类型转换和计算，结果存储在orderDetail表中
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +
                        "  after['order_id'] as order_id," +
                        "  after['sku_id'] as sku_id," +
                        "  after['sku_name'] as sku_name," +
                        "  after['create_time'] as create_time," +
                        "  after['source_id'] as source_id," +
                        "  after['source_type'] as source_type," +
                        "  after['sku_num'] as sku_num," +
                        "  cast(cast(after['sku_num'] as decimal(16,2)) * " +
                        "  cast(after['order_price'] as decimal(16,2)) as String) as split_original_amount," + // 分摊原始总金额
                        "  after['split_total_amount'] as split_total_amount," +  // 分摊总金额
                        "  after['split_activity_amount'] as split_activity_amount," + // 分摊活动金额
                        "  after['split_coupon_amount'] as split_coupon_amount," + // 分摊的优惠券金额
                        "  ts_ms " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail' " +
                        "  and `op`='r' ");
// 将orderDetail表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_detail", orderDetail);
// 注释掉的代码，若取消注释，会执行查询并打印orderDetail表的数据，用于调试
 orderDetail.execute().print();
// TODO 过滤出订单数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_info'且操作类型为'r'的数据
// 提取出相关字段，如id、user_id、province_id，结果存储在orderInfo表中
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +
                        "  after['user_id'] as user_id," +
                        "  after['province_id'] as province_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_info' " +
                        "  and `op`='r' ");
// 将orderInfo表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_info", orderInfo);
// 注释掉的代码，若取消注释，会执行查询并打印orderInfo表的数据，用于调试
 orderInfo.execute().print();

// TODO 过滤出明细活动数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_detail_activity'且操作类型为'r'的数据
// 提取出相关字段，如order_detail_id、activity_id、activity_rule_id，结果存储在orderDetailActivity表中
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +
                        "  after['activity_id'] activity_id, " +
                        "  after['activity_rule_id'] activity_rule_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_activity' " +
                        "  and `op` = 'r' ");
// 将orderDetailActivity表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
// 注释掉的代码，若取消注释，会执行查询并打印orderDetailActivity表的数据，用于调试
 orderDetailActivity.execute().print();

// TODO 过滤出明细优惠券数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_detail_coupon'且操作类型为'r'的数据
// 提取出相关字段，如order_detail_id、coupon_id，结果存储在orderDetailCoupon表中
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +
                        "  after['coupon_id'] coupon_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_coupon' " +
                        "  and `op` = 'r' ");
// 将orderDetailCoupon表注册为临时视图，方便后续的SQL查询引用
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
// 注释掉的代码，若取消注释，会执行查询并打印orderDetailCoupon表的数据，用于调试
 orderDetailCoupon.execute().print();

// TODO 关联上述4张表
// 在order_detail、order_info、order_detail_activity和order_detail_coupon临时视图上执行SQL查询，进行表连接操作
// 连接条件为相关字段相等，提取出连接后的相关字段，结果存储在result表中
// 对create_time字段进行转换处理，生成date_id字段
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  od.id," +
                        "  od.order_id," +
                        "  oi.user_id," +
                        "  od.sku_id," +
                        "  od.sku_name," +
                        "  oi.province_id," +
                        "  act.activity_id," +
                        "  act.activity_rule_id," +
                        "  cou.coupon_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  od.create_time," +
                        "  od.sku_num," +
                        "  od.split_original_amount," +
                        "  od.split_activity_amount," +
                        "  od.split_coupon_amount," +
                        "  od.split_total_amount," +
                        "  od.ts_ms " +
                        "  from order_detail od " +
                        "  join order_info oi on od.order_id = oi.id " +
                        "  left join order_detail_activity act " +
                        "  on od.id = act.order_detail_id " +
                        "  left join order_detail_coupon cou " +
                        "  on od.id = cou.order_detail_id ");
// 注释掉的代码，若取消注释，会执行查询并打印result表的数据，用于调试
 result.execute().print();

// TODO 将关联的结果写到Kafka主题
// 在流表环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_ORDER_DETAIL的表
// 该表用于将处理后的数据写入Kafka，表结构包含众多字段，id为主键（但不强制约束）
// SQLUtil.getUpsertKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了Kafka的主题和相关配置
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts_ms bigint," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

// 将result表中的数据插入到TOPIC_DWD_TRADE_ORDER_DETAIL表中，即将处理后的数据写入对应的Kafka主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

// 执行Flink作业，作业名称为DwdOrderFactSheet
        env.execute("DwdOrderFactSheet");

    }
}
