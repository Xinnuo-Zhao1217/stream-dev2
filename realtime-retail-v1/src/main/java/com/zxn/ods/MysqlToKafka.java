package com.zxn.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zxn.ods.MysqlToKafka
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:11
 * @description: 实现了从 MySQL 数据库到 Kafka 消息队列的实时数据同步，能让 Kafka 接收 MySQL
 * 数据库的变更数据，为后续的数据处理和分析提供支持。
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // 使用自定义工具类获取MySQL数据源，指定数据库为"realtime_v1"，表名为所有表（用"*"表示）
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");
        // 从MySQL数据源创建DataStreamSource，不使用水位线，源名称为"MySQL Source"
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();


//        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("xinnuo_zhao_db1");

//        mySQLSource.sinkTo(topic_db);


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
