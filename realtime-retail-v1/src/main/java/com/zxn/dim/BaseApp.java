package com.zxn.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zxn.bean.TableProcessDim;
import com.zxn.constant.Constant;
import com.zxn.fonction.HBaseSinkFunction;
import com.zxn.fonction.TableProcessFunction;
import com.zxn.util.FlinkSourceUtil;
import com.zxn.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.zxn.stream.realtime.v1.app.bim.BaseApp
 * @Author xinnuo.zhao
 * @Date 2025/4/11 9:34
 * @description: BaseApp
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4
        env.setParallelism(4);
        // 启用检查点，间隔为5000毫秒，检查点模式为精确一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 获取Kafka数据源，使用自定义工具类获取，指定主题为Constant.TOPIC_DB，消费者组为"dim_app"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
        // 从Kafka源创建DataStreamSource，不使用水位线，源名称为"kafka_source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        // 注释掉的代码，原本功能可能是将Kafka读取的数据打印到控制台，此处先注释
        // kafkaStrDS.print();
        // 对Kafka读取的字符串流进行处理，转换为JSONObject类型的单输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        // 处理函数，接收输入字符串、上下文和输出收集器
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        // 从解析后的JSONObject对象中获取"source"对象下的"db"字段值
                        String db = jsonObj.getJSONObject("source").getString("db");
//                        // 从解析后的JSONObject对象中获取"op"字段值
                        String type = jsonObj.getString("op");
                        // 从解析后的JSONObject对象中获取"after"字段值
                        String data = jsonObj.getString("after");
                        // 判断条件：db为"realtime_v1"，且type为"c"、"u"、"d"、"r"中的一个，且data不为空
                        if ("realtime_v1".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

//        jsonObjDS.print();
// 获取MySQL数据源，使用自定义工具类获取，指定数据库为"realtime_v1_config"，表名为"table_process_dim"
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");
// 从MySQL源创建DataStreamSource，不使用水位线，源名称为"mysql_source"，并设置并行度为1
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

// 注释掉的代码，原本功能可能是将MySQL读取的数据打印到控制台，此处先注释
// mysqlStrDS.print();

// 对从MySQL读取的字符串流进行映射转换，转换为TableProcessDim类型的单输出流
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        // 将输入的JSON字符串解析为JSONObject对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 从解析后的JSONObject对象中获取"op"字段值
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        // 如果操作类型为"d"（删除操作）
                        if ("d".equals(op)) {
                            // 从JSONObject对象的"before"字段中获取数据，并转换为TableProcessDim对象
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            // 对于其他操作类型，从JSONObject对象的"after"字段中获取数据，并转换为TableProcessDim对象
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        // 设置操作类型到TableProcessDim对象中
                        tableProcessDim.setOp(op);
                        // 返回转换并设置好操作类型的TableProcessDim对象
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

// 注释掉的代码，原本功能可能是将转换后的TableProcessDim数据打印到控制台，此处先注释
// tpDS.print();



        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    // 用于存储HBase连接对象
                    private Connection hbaseConn;

                    // 重写open方法，在算子初始化时执行，用于获取HBase连接
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    // 重写close方法，在算子结束时执行，用于关闭HBase连接
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    // 重写map方法，对输入的TableProcessDim对象进行处理
                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp(); // 获取操作类型
                        String sinkTable = tp.getSinkTable(); // 获取目标表名
                        // 将目标表的列族字符串按逗号分割成数组
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) { // 如果是删除操作
                            // 删除HBase中对应的表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) { // 如果是读或创建操作
                            // 在HBase中创建对应的表，传入连接、命名空间、表名和列族数组
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp; // 返回处理后的TableProcessDim对象
                    }
                }
        );

//         tpDS.print();

        // 注释掉的代码，原本功能可能是将tpDS中的数据打印到控制台，此处先注释
// tpDS.print();

// 创建一个MapState描述符，用于管理键值对状态，键为String类型，值为TableProcessDim类型
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
// 将tpDS流广播出去，使用前面创建的MapState描述符来管理广播状态
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

// 将之前处理得到的jsonObjDS流与广播流broadcastDS连接起来，得到连接流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

// 对连接流进行处理，使用自定义的TableProcessFunction，并传入MapState描述符
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

// 注释掉的代码，原本功能可能是将dimDS中的数据打印到控制台，此处先注释
// dimDS.print();

// 为dimDS流添加一个HBaseSinkFunction，用于将数据写入HBase
        dimDS.addSink(new HBaseSinkFunction());

// 执行Flink作业，作业名称为"dim"
        env.execute("dim");

    }
}
