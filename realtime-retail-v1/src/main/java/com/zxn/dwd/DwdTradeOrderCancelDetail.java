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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

// 在流表环境中创建名为topic_db的表，用于从Kafka读取数据

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

 tableEnv.executeSql("select * from topic_db").print();

// 在流表环境中创建名为base_dic的表，用于从HBase读取数据

        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );

 tableEnv.executeSql("select * from base_dic").print();

// 在topic_db表上执行SQL查询，筛选出与订单取消相关的数据

        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +
                " `after`['operate_time'] as operate_time, " +
                " `ts_ms` " +
                " from topic_db " +
                " where source['table'] = 'order_info' " +
                " and `op` = 'r' " +
                " and `after`['order_status'] = '1001' " +
                " or `after`['order_status'] = '1003' ");

        tableEnv.createTemporaryView("order_cancel", orderCancel);

 orderCancel.execute().print();

// 在流表环境中创建名为dwd_trade_order_detail的表，用于从Kafka读取下单事实表数据

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

 result.execute().print();

// 在流表环境中创建名为TOPIC_DWD_TRADE_ORDER_CANCEL的表，用于将处理后的数据写入Kafka

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

        env.execute("DwdTradeOrderCancelDetail");
    }
}
