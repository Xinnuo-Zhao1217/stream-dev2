package com.zxn.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.zxn.func.AgeScoreFunc
 * @Author zhao.xinnuo
 * @Date 2025/5/15 10:04
 * @description: AgeScoreFunc
 */
public class AgeScoreFunc extends RichMapFunction<JSONObject,JSONObject> {
    // 四舍五入到三位小数的工具方法
    private double roundToThreeDecimals(double value) {
        return new BigDecimal(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) {
        //获取两条JSON对象
        JSONObject user = jsonObject.getJSONObject("user");
        JSONObject age = jsonObject.getJSONObject("Age");
        // 创建一个新的JSON对象,用于手机Age分数
        JSONObject ageScore = new JSONObject();

        // 类目
        Set<String> TrendCategoryValues = new HashSet<>(Arrays.asList("珠宝", "礼品箱包", "鞋靴", "服饰内衣", "个护化妆", "数码"));
        Set<String> HomeCategoryValues = new HashSet<>(Arrays.asList("母婴", "钟表", "厨具", "电脑办公", "家居家装", "家用电器", "图书、音像、电子书刊", "手机", "汽车用品"));
        Set<String> HealthCategoryValues = new HashSet<>(Arrays.asList("运动健康", "食品饮料、保健食品"));
        String categoryName = age.getString("category_name");
        //品牌
        Set<String> EquipmentTmValues = new HashSet<>(Arrays.asList("Redmi", "苹果", "联想", "TCL", "小米"));
        Set<String> TrendTmValues = new HashSet<>(Arrays.asList("长粒香", "金沙河", "索芙特", "CAREMiLLE", "欧莱雅", "香奈儿"));
        String tmName = age.getString("tm_name");
        //价格
        String priceSensitive = age.getString("price_sensitive");
        // 时间
        String timePeriod = age.getString("time_period");
        // 搜索词
        String keywordType = age.getString("keyword_type");
        // 设备信息
        String os = age.getString("os");

        // 获取初始年龄段
        String ageGroup = user.getString("ageGroup");
        // 计算算法年龄段
        if (ageGroup != null && ageGroup.equals("18-24")){
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.1 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.9 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.1 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
            }

            // 判断时间段

            switch (timePeriod) {
                case "凌晨":
                case "上午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "下午":
                case "中午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "早晨":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "晚上":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.8 * 0.1));
                    break;
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.9 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.9 * 0.15));
                    break;
                case "性价比":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.8 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.7 * 0.1));
            }
        } else if (ageGroup != null && ageGroup.equals("25-29")) {
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.7 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.3 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
            }

            // 判断时间段
            switch (timePeriod) {
                case "凌晨":
                case "早晨":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "晚上":
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.7 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "性价比":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.7 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.6 * 0.1));
            }
        } else if (ageGroup != null && ageGroup.equals("30-34")) {
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.5 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.5 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }

            // 判断时间段
            switch (timePeriod) {
                case "凌晨":
                case "早晨":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "晚上":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.6 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
                case "性价比":
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.6 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.5 * 0.1));
            }
        } else if (ageGroup != null && ageGroup.equals("35-39")) {
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.3 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.7 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
            }

            // 判断时间段
            switch (timePeriod) {
                case "凌晨":
                case "早晨":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                case "晚上":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
                case "性价比":
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.5 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.4 * 0.1));
            }
        } else if (ageGroup != null && ageGroup.equals("40-49")) {
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.2 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.8 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
            }

            // 判断时间段
            switch (timePeriod) {
                case "凌晨":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "早晨":
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "上午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
                case "中午":
                case "晚上":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "性价比":
                case "健康与养生":
                case "家庭与育儿":
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.4 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.3 * 0.1));
            }
        } else if (ageGroup != null && ageGroup.equals("50+")) {
            // 判断品类
            if (TrendCategoryValues.contains(categoryName)){
                ageScore.put("category_score", roundToThreeDecimals(0.1 * 0.3));
            } else if (HomeCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.7 * 0.3));
            } else if (HealthCategoryValues.contains(categoryName)) {
                ageScore.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            }

            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.1 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                ageScore.put("tm_score", roundToThreeDecimals(0.9 * 0.2));
            }

            // 判断价格敏感度
            switch (priceSensitive) {
                case "高价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "中价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "低价商品":
                    ageScore.put("price_sensitive_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
            }

            // 判断时间段
            switch (timePeriod) {
                case "凌晨":
                case "夜间":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "早晨":
                case "晚上":
                case "中午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
                case "上午":
                case "下午":
                    ageScore.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
            }

            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "性价比":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "健康与养生":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.9 * 0.15));
                    break;
                case "家庭与育儿":
                case "学习与发展":
                    ageScore.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
            }

            // 判断设备信息
            if (os.equals("Android")){
                ageScore.put("os_score", roundToThreeDecimals(0.3 * 0.1));
            } else if (os.equals("iOS")) {
                ageScore.put("os_score", roundToThreeDecimals(0.2 * 0.1));
            }
        }else {
            ageScore.put("category_score", -1 );
        }

        jsonObject.put("ageScore", ageScore);
        return jsonObject;
    }
}

