package com.zxn.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.zxn.ods.MysqlToKafka
 * @Author zhao.xinnuo
 * @Date 2025/5/12 10:12
 * @description: MysqlToKafka
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //实用工具类获取MySQL数据源
        MySqlSource<String> realtime_v1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");
        //自定义数据源
        final DataStreamSource<String> mySQLSource = env.fromSource(realtime_v1, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        mySQLSource.print();

        SingleOutputStreamOperator<JSONObject> stre = mySQLSource.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        SingleOutputStreamOperator<JSONObject> user = stre.map(jsonStr -> {
            JSONObject json = JSON.parseObject(String.valueOf(jsonStr));
            JSONObject after = json.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer epochDay = after.getInteger("birthday");
                if (epochDay != null) {
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                }
            }
            return json;
        });
        user.print();

        env.execute();
    }
}
