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
 * @description: 从 Kafka 主题中读取购物车相关的数据，经过处理后再将数据写入到另一个 Kafka 主题中
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

// 在表执行环境中执行SQL语句，创建名为topic_db的表
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));


 tableEnv.executeSql("select * from topic_db").print();

// 在表执行环境中执行SQL语句，创建名为base_dic的表

        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );


 tableEnv.executeSql("select * from base_dic").print();

// 在表执行环境中执行SQL查询，从topic_db表筛选出与购物车信息相关的数据
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


 cartInfo.execute().print();
// 在表执行环境中执行SQL语句，创建名为TOPIC_DWD_TRADE_CART_ADD的表
        tableEnv.executeSql(" create table "+ Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));


        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

        env.execute("dwd_kafka");
    }
}
