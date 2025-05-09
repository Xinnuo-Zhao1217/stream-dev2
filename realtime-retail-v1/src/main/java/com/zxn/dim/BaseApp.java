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
 * @Date 2025/5/4 9:34
 * @description: 实现了从 Kafka 和 MySQL 读取数据，根据数据操作类型对数据进行处理和 HBase 表操作，
 * 并将处理后的数据写入 HBase 的功能
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // kafkaStrDS.print();
        // 对Kafka读取的字符串流进行处理，转换为JSONObject类型的单输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
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

// mysqlStrDS.print();

// 对从MySQL读取的字符串流进行映射转换，转换为TableProcessDim类型的单输出流
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        // 如果操作类型为"d"（删除操作）
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

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
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        // 将目标表的列族字符串按逗号分割成数组
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            // 删除HBase中对应的表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            // 在HBase中创建对应的表，传入连接、命名空间、表名和列族数组
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        );

//         tpDS.print();


// tpDS.print();


        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
// 将tpDS流广播出去，使用前面创建的MapState描述符来管理广播状态
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

// dimDS.print();

// 为dimDS流添加一个HBaseSinkFunction，用于将数据写入HBase
        dimDS.addSink(new HBaseSinkFunction());

        env.execute("dim");

    }
}
