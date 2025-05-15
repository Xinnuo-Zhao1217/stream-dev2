package com.zxn.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.zxn.func.LogFilterFunc
 * @Author zhao.xinnuo
 * @Date 2025/5/15 10:01
 * @description: LogFilterFunc
 */
public class LogFilterFunc extends RichMapFunction<JSONObject,JSONObject> {

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject object = new JSONObject();
        if (jsonObject.containsKey("common")){
            JSONObject common = jsonObject.getJSONObject("common");
            object.put("uid",common.getString("uid"));
            object.put("ts",jsonObject.getLong("ts"));
            // 去掉版本直接获取 OS
            String os = common.getString("os").split(" ")[0];
            object.put("os",os);
            if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                JSONObject page = jsonObject.getJSONObject("page");
                if (page.containsKey("item_type") && page.getString("item_type").equals("keyword")){
                    object.put("keyword",page.getString("item"));
                }
            }
        }
        return object;
    }
}

