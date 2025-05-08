package com.zxn.dwd;

import com.zxn.constant.Constant;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdTradeRefundPaySucDetail
 * @Author zhao.xinnuo
 * @Date 2025/5/4 22:57
 * @description: 处理电商交易中退款支付成功的详细数据
 */
public class DwdTradeRefundPaySucDetail {
    public static void main(String[] args) throws Exception {
        // 获取Flink的流执行环境，后续用于构建和运行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置任务的并行度为4，意味着作业将使用4个并行任务来处理数据
        env.setParallelism(4);
// 创建流表执行环境，以便在流处理过程中使用SQL语句操作数据
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 启用检查点机制，每隔5000毫秒（5秒）进行一次检查点操作，采用精确一次语义
// 这样能确保在故障恢复时数据处理的一致性，不会出现数据重复或丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
// 设置空闲状态的保留时间为30分钟零5秒，用于管理流处理中的状态数据
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

// 在流表环境中执行SQL语句，创建名为topic_db的表
// 该表用于从Kafka读取数据，包含after（键值对映射）、source（键值对映射）、op（操作类型字符串）和ts_ms（时间戳长整型）字段
// SQLUtil.getKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了相关的Kafka主题
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
// 注释掉的代码，若取消注释会执行查询并打印topic_db表的所有数据，用于调试
 tableEnv.executeSql("select * from topic_db").print();

// 在流表环境中执行SQL语句，创建名为base_dic的表
// 该表用于从HBase读取数据，包含dic_code（字典代码字符串）、info（包含dic_name的行类型）字段，dic_code为主键（但不强制约束）
// SQLUtil.getHBaseDDL方法用于获取创建HBase表的DDL语句，指定了HBase表名
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
// 注释掉的代码，若取消注释会执行查询并打印base_dic表的所有数据，用于调试
// tableEnv.executeSql("select * from base_dic").print();

// 3. 过滤退款成功表数据
// 在topic_db表上执行SQL查询，筛选出source表名为'refund_payment'、操作类型为'r'、退款状态不为空且退款状态为'1602'的数据
// 提取出相关字段，如id、order_id、sku_id等，结果存储在refundPayment表中
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['payment_type'] payment_type," +
                        " after['callback_time'] callback_time," +
                        " after['total_amount'] total_amount," +
                        " ts_ms " +
                        " from topic_db " +
                        " where source['table']='refund_payment' " +
                        " and `op`='r' " +
                        " and `after`['refund_status'] is not null " +
                        " and `after`['refund_status']='1602'");
// 将refundPayment表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("refund_payment", refundPayment);
// 注释掉的代码，若取消注释会执行查询并打印refundPayment表的数据，用于调试
// refundPayment.execute().print();

// 4. 过滤退单表中的退单成功的数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_refund_info'、操作类型为'r'、退款状态不为空且退款状态为'0705'的数据
// 提取出相关字段，如order_id、sku_id、refund_num，结果存储在orderRefundInfo表中
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['refund_num'] refund_num " +
                        " from topic_db " +
                        " where source['table']='order_refund_info' " +
                        " and `op`='r' " +
                        " and `after`['refund_status'] is not null " +
                        " and `after`['refund_status']='0705'");
// 将orderRefundInfo表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
// 注释掉的代码，若取消注释会执行查询并打印orderRefundInfo表的数据，用于调试
// orderRefundInfo.execute().print();

// 5. 过滤订单表中的退款成功的数据
// 在topic_db表上执行SQL查询，筛选出source表名为'order_info'、操作类型为'r'、订单状态不为空且订单状态为'1006'的数据
// 提取出相关字段，如id、user_id、province_id，结果存储在orderInfo表中
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_db " +
                        "where source['table']='order_info' " +
                        "and `op`='r' " +
                        "and `after`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
// 将orderInfo表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("order_info", orderInfo);
// 注释掉的代码，若取消注释会执行查询并打印orderInfo表的数据，用于调试
// orderInfo.execute().print();

// 6. 4 张表的 join
// 在refund_payment、order_refund_info和order_info临时视图上执行SQL查询，进行表连接操作
// 连接条件为refund_payment表的order_id和sku_id分别与order_refund_info表的order_id和sku_id相等，
// 以及refund_payment表的order_id与order_info表的id相等
// 提取出连接后的相关字段，如id、user_id、order_id等，并对callback_time字段进行转换处理生成date_id字段，结果存储在result表中
        Table result = tableEnv.sqlQuery(
                "select " +
                        " rp.id," +
                        " oi.user_id," +
                        " rp.order_id," +
                        " rp.sku_id," +
                        " oi.province_id," +
                        " rp.payment_type," +
                        " date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(rp.callback_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        " rp.callback_time," +
                        " ori.refund_num," +
                        " rp.total_amount," +
                        " rp.ts_ms " +
                        " from refund_payment rp " +
                        " join order_refund_info ori " +
                        " on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        " join order_info oi " +
                        " on rp.order_id=oi.id ");
// 注释掉的代码，若取消注释会执行查询并打印result表的数据，用于调试
// result.execute().print();

// 7.写出到 kafka
// 在流表环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS的表
// 该表用于将处理后的数据写入Kafka，包含id、user_id、order_id等字段，id为主键（但不强制约束）
// SQLUtil.getUpsertKafkaDDL方法用于获取创建Kafka表的DDL语句，指定了相关的Kafka主题
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                " id string," +
                " user_id string," +
                " order_id string," +
                " sku_id string," +
                " province_id string," +
                " payment_type_code string," +
                " date_id string," +
                " callback_time string," +
                " refund_num string," +
                " refund_amount string," +
                " ts_ms bigint ," +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
// 将result表中的数据插入到TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS表中，即将处理后的数据写入对应的Kafka主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

// 执行Flink作业，作业名称为DwdTradeRefundPaySucDetail
        env.execute("DwdTradeRefundPaySucDetail");


    }
}
