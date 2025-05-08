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
        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为2
        env.setParallelism(2);

        // 使用自定义工具类获取MySQL数据源，指定数据库为"realtime_v1"，表名为所有表（用"*"表示）
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");
        // 从MySQL数据源创建DataStreamSource，不使用水位线，源名称为"MySQL Source"
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");
        // 注释掉的代码，原本功能可能是将MySQL读取的数据打印到控制台，此处先注释
        mySQLSource.print();

        // 使用自定义工具类获取Kafka sink，指定Kafka主题为"xinnuo_zhao_db"
//        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("xinnuo_zhao_db1");
//        // 将MySQL数据源的数据写入到指定的Kafka主题
//        mySQLSource.sinkTo(topic_db);

        // 执行Flink作业，作业名称为"Print MySQL Snapshot + Binlog"
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
