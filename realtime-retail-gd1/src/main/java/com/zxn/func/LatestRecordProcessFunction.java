package com.zxn.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.zxn.func.LatestRecordProcessFunction
 * @Author zhao.xinnuo
 * @Date 2025/5/15 10:02
 * @description: LatestRecordProcessFunction
 */
public  class LatestRecordProcessFunction extends KeyedProcessFunction<Integer, JSONObject, JSONObject> {

    private transient ValueState<JSONObject> maxState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<JSONObject> descriptor =
                new ValueStateDescriptor<>("maxRecord", JSONObject.class);
        maxState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject current, Context ctx, Collector<JSONObject> out) throws Exception {


        long currentTs = current.getLong("ts");

        JSONObject maxRecord = maxState.value();
        if (maxRecord == null) {
            maxState.update(current);
            out.collect(current);
            return;
        }

        long maxTs = maxRecord.getLong("ts");
        if (currentTs > maxTs) {
            maxState.update(current);
            out.collect(current);
        }
    }
}

