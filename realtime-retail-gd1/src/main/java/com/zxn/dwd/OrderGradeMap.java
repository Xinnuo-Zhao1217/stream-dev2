package com.zxn.dwd;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;

/**
 * @Package com.rb.test_dm.true_a.OrderGradeMap
 * @Author xinnuo.zhao
 * @Date 2025/5/14 14:01
 * @description:
 */

public class OrderGradeMap extends RichMapFunction<JSONObject,JSONObject> {

    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;
    private static final String c1Path= "D:\\idea\\stream-dev2\\realtime-retail-gd1\\src\\main\\java\\com\\zxn\\dom/c1Weight.txt";
    private static final String pricePath= "D:\\idea\\stream-dev2\\realtime-retail-gd1\\src\\main\\java\\com\\zxn\\dom/priceWeight.txt";
    private static final String timePath= "D:\\idea\\stream-dev2\\realtime-retail-gd1\\src\\main\\java\\com\\zxn\\dom/timeWeight.txt";
    private static final String tmPath= "D:\\idea\\stream-dev2\\realtime-retail-gd1\\src\\main\\java\\com\\zxn\\dom/tmWeight.txt";
    private static  HashMap<String,JSONObject> c1Map ;
    private static  HashMap<String,JSONObject> tmMap ;
    private static  HashMap<String,JSONObject> timeMap ;
    private static  HashMap<String,JSONObject> priceMap ;
    static {
       c1Map = ReadToJson.readFileToJsonMap(c1Path);
       tmMap = ReadToJson.readFileToJsonMap(tmPath);
       timeMap = ReadToJson.readFileToJsonMap(timePath);
       priceMap = ReadToJson.readFileToJsonMap(pricePath);
    }

    @Override
    public JSONObject map(JSONObject value) throws Exception {
        //时间数据
        String timeType = value.getString("time_type");
        JSONObject timeObj = timeMap.get(timeType);
        value.put("time_code", timeObj);

        //价格区间
        String priceLevel = value.getString("price_level");
        JSONObject priceObj = priceMap.get(priceLevel);
        value.put("price_code", priceObj);

        //类目
        String c1 = value.getString("c1_name");
        JSONObject c1Obj = c1Map.get(c1);
        value.put("c1_code", c1Obj);

        //品牌
        String tmName = value.getString("tm_name");
        JSONObject tmObj = tmMap.get(tmName);
        if (tmObj!=null){
            value.put("tm_code", tmObj);
        }else {
            value.put("tm_code", tmMap.get("香奈儿"));
        }


        return value;
    }
}
