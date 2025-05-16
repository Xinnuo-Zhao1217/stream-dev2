package com.zxn.funcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.constant.Constant;
import com.zxn.util.ConfigUtils;
import com.zxn.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

/**
 * Flink作业：从Kafka读取基础标签数据，进行关联计算
 * @Author xinnuo.zhao
 * @Date 2025/5/15 15:32
 */
public class DbusUserLabel6BaseCalculate {
    private static final String KAFKA_BOOTSTRAP_SERVERS = Constant.KAFKA_BROKERS;
    private static final String KAFKA_LABEL_BASE6_TOPIC = ConfigUtils.getString("kafka.result.label.base6.topic");
    private static final String KAFKA_LABEL_BASE4_TOPIC = ConfigUtils.getString("kafka.result.label.base4.topic");
    private static final String KAFKA_LABEL_BASE2_TOPIC = ConfigUtils.getString("kafka.result.label.base2.topic");
    private static final String CONSUMER_GROUP_ID = "label-calculate-group";

    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);

        // 构建Kafka数据源
        SingleOutputStreamOperator<String> kafkaBase6Source = createKafkaSource(env, KAFKA_LABEL_BASE6_TOPIC, "base6-source");
        SingleOutputStreamOperator<String> kafkaBase4Source = createKafkaSource(env, KAFKA_LABEL_BASE4_TOPIC, "base4-source");
        SingleOutputStreamOperator<String> kafkaBase2Source = createKafkaSource(env, KAFKA_LABEL_BASE2_TOPIC, "base2-source");

        // 解析JSON数据
        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(jsonParser());
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(jsonParser());
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(jsonParser());

        // 2级与4级标签关联
        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinBase2And4BaseFunc());

        // 重新分配时间戳和水印
        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));

        // 关联6级标签
        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessLabelFunc());

        // 输出结果
        userLabelProcessDs.print();

        // 执行作业
        env.execute("User Label Calculation Job");
    }

    /**
     * 创建Kafka数据源
     */
    private static SingleOutputStreamOperator<String> createKafkaSource(
            StreamExecutionEnvironment env, String topic, String sourceName) {
        return env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                KAFKA_BOOTSTRAP_SERVERS,
                                topic,
                                CONSUMER_GROUP_ID + "-" + topic,
                                OffsetsInitializer.earliest()
                        ),
                        createWatermarkStrategy(),
                        sourceName
                ).uid(sourceName + "-uid")
                .name(sourceName);
    }

    /**
     * 创建水印策略
     */
    private static WatermarkStrategy<String> createWatermarkStrategy() {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(event);
                        if (jsonObject != null && jsonObject.containsKey("ts_ms")) {
                            return jsonObject.getLong("ts_ms");
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                    }
                    return 0L;
                });
    }

    /**
     * JSON解析器
     */
    private static MapFunction<String, JSONObject> jsonParser() {
        return JSON::parseObject;
    }
}