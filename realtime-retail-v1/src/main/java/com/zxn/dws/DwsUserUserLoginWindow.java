package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.UserLoginBean;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.util.DateFormatUtil;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.zxn.dws.DwsUserUserLoginWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:09
 * @description: 实现了一个完整的 Flink 流处理作业，从 Kafka 读取用户登录数据，
 * 经过一系列处理后，将新用户和回流用户的统计结果按窗口聚合，并写入 Doris 数据库。
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        // 获取Flink流执行环境，用于后续构建和执行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业的并行度为1，即所有任务在一个并行实例上执行
        env.setParallelism(1);

        // 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次（EXACTLY_ONCE）语义
        // 确保在故障恢复时数据处理的一致性，无数据重复或丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略为固定延迟重启，最多重启3次，每次重启间隔3000毫秒（3秒）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从自定义工具类FlinkSourceUtil获取Kafka数据源，指定Kafka主题为"dwd_traffic_page_xinnuo_zhao"
        // 消费者组ID为"dws_user_user_login_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page_chenming", "dws_user_user_login_window");

        // 从Kafka数据源创建数据流，不使用水位线策略，命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 将从Kafka读取的字符串数据转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 注释掉的打印语句，用于调试查看jsonObjDS中的数据
        jsonObjDS.print();

        // 对数据进行过滤，只保留用户ID不为空且上一页ID为"login"或为空的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        // 从JSONObject的"common"字段中获取用户ID（uid）
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        // 从JSONObject的"page"字段中获取上一页ID（lastPageId）
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        // 判断用户ID不为空且上一页ID为"login"或为空，满足条件则返回true保留数据
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看filterDS中的数据
        filterDS.print();

        // 为过滤后的数据指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // 从JSONObject中提取"ts"字段作为事件时间戳
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // 按照用户ID（uid）对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        // 使用Flink的状态编程，判断用户是否为新用户（首次登录）或回流用户，并封装为UserLoginBean对象
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    // 定义状态变量，存储用户上次登录日期
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符，指定状态名称和类型
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        // 从运行时上下文获取状态对象
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                        // 从状态中获取用户上次登录日期
                        String lastLoginDate = lastLoginDateState.value();

                        // 从JSONObject中获取当前时间戳
                        Long ts = jsonObj.getLong("ts");
                        // 将当前时间戳转换为日期字符串
                        String curLoginDate = DateFormatUtil.tsToDate(ts);

                        // 初始化新用户计数（uuCt）和回流用户计数（backCt）为0
                        long uuCt = 0L;
                        long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            // 如果上次登录日期不为空，判断当前登录日期与上次登录日期是否不同
                            if (!lastLoginDate.equals(curLoginDate)) {
                                // 若不同，则新用户计数加1，并更新上次登录日期状态
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                // 计算距离上次登录的天数
                                long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                // 如果距离上次登录天数大于等于8天，则认为是回流用户，回流用户计数加1
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            // 如果上次登录日期为空，则认为是新用户，新用户计数加1，并更新上次登录日期状态
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        // 如果新用户计数不为0，则将相关数据封装为UserLoginBean对象并输出
                        if (uuCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看beanDS中的数据
        beanDS.print();

        // 对数据应用滚动事件时间窗口，窗口大小为10秒
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 对窗口内的数据进行聚合操作
        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                // 定义ReduceFunction，用于累加新用户计数和回流用户计数
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                        // 累加新用户计数
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        // 累加回流用户计数
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                // 定义AllWindowFunction，用于处理窗口聚合后的结果
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        // 获取窗口内的第一个UserLoginBean对象
                        UserLoginBean bean = values.iterator().next();
                        // 将窗口开始时间转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        // 将窗口结束时间转换为日期时间字符串
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        // 将窗口开始时间转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        // 设置UserLoginBean对象的相关时间字段
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        // 输出处理后的UserLoginBean对象
                        out.collect(bean);
                    }
                }
        );

        // 将UserLoginBean对象转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS.map(new BeanToJsonStrMapFunction<>());

        // 打印转换后的JSON字符串，用于调试
        jsonMap.print();

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

        // 执行Flink作业，作业名称为"DwsUserUserLoginWindow"
        env.execute("DwsUserUserLoginWindow");
    }
}
