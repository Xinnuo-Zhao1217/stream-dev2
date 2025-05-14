package com.zxn.dwd;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zxn.dwd.SearchWordAnalysis
 * @Author zhao.xinnuo
 * @Date 2025/5/14 10:56
 * @description: 该类用于对搜索词进行分析，计算各年龄段对不同搜索词类别的偏好情况，增加权重计算功能
 */
public class SearchWordAnalysis {
    public static void main(String[] args) {
        // 初始化搜索词分析数据
        // 创建一个外层Map，键为年龄段，值为内层Map，内层Map用于存储该年龄段对不同搜索词类别的偏好值
        Map<String, Map<String, Double>> searchData = new HashMap<>();

        // 初始化18 - 24岁年龄段的搜索词偏好数据
        Map<String, Double> age18_24 = new HashMap<>();
        age18_24.put("时尚与潮流", 0.9);
        age18_24.put("性价比", 0.2);
        age18_24.put("健康与养生", 0.1);
        age18_24.put("家庭与育儿", 0.1);
        age18_24.put("科技与数码", 0.8);
        age18_24.put("学习与发展", 0.4);
        // 将18 - 24岁年龄段的数据放入总的搜索词分析数据中
        searchData.put("18-24岁", age18_24);

        // 按照同样方式初始化其他年龄段的数据
        // 初始化25 - 29岁年龄段的搜索词偏好数据
        Map<String, Double> age25_29 = new HashMap<>();
        age25_29.put("时尚与潮流", 0.7);
        age25_29.put("性价比", 0.4);
        age25_29.put("健康与养生", 0.2);
        age25_29.put("家庭与育儿", 0.2);
        age25_29.put("科技与数码", 0.6);
        age25_29.put("学习与发展", 0.5);
        searchData.put("25-29岁", age25_29);

        // 初始化30 - 34岁年龄段的搜索词偏好数据
        Map<String, Double> age30_34 = new HashMap<>();
        age30_34.put("时尚与潮流", 0.5);
        age30_34.put("性价比", 0.6);
        age30_34.put("健康与养生", 0.4);
        age30_34.put("家庭与育儿", 0.4);
        age30_34.put("科技与数码", 0.4);
        age30_34.put("学习与发展", 0.6);
        searchData.put("30-34岁", age30_34);

        // 初始化35 - 39岁年龄段的搜索词偏好数据
        Map<String, Double> age35_39 = new HashMap<>();
        age35_39.put("时尚与潮流", 0.3);
        age35_39.put("性价比", 0.7);
        age35_39.put("健康与养生", 0.6);
        age35_39.put("家庭与育儿", 0.6);
        age35_39.put("科技与数码", 0.3);
        age35_39.put("学习与发展", 0.7);
        searchData.put("35-39岁", age35_39);

        // 初始化40 - 49岁年龄段的搜索词偏好数据
        Map<String, Double> age40_49 = new HashMap<>();
        age40_49.put("时尚与潮流", 0.2);
        age40_49.put("性价比", 0.8);
        age40_49.put("健康与养生", 0.8);
        age40_49.put("家庭与育儿", 0.8);
        age40_49.put("科技与数码", 0.2);
        age40_49.put("学习与发展", 0.8);
        searchData.put("40-49岁", age40_49);

        // 初始化50岁以上年龄段的搜索词偏好数据
        Map<String, Double> age50Above = new HashMap<>();
        age50Above.put("时尚与潮流", 0.1);
        age50Above.put("性价比", 0.8);
        age50Above.put("健康与养生", 0.9);
        age50Above.put("家庭与育儿", 0.7);
        age50Above.put("科技与数码", 0.1);
        age50Above.put("学习与发展", 0.7);
        searchData.put("50岁以上", age50Above);

        // 假设为不同搜索词类别设置的权重
        Map<String, Double> weights = new HashMap<>();
        weights.put("时尚与潮流", 0.8);
        weights.put("性价比", 0.6);
        weights.put("健康与养生", 0.7);
        weights.put("家庭与育儿", 0.5);
        weights.put("科技与数码", 0.9);
        weights.put("学习与发展", 0.7);

        // 计算各年龄段对所有搜索词类别的加权偏好总和
        Map<String, Double> weightedTotalPreferencePerAge = calculateWeightedTotalPreferencePerAge(searchData, weights);
        System.out.println("各年龄段对所有搜索词类别的加权偏好总和:");
        weightedTotalPreferencePerAge.forEach((age, total) -> System.out.println(age + ": " + total));

        // 找出每个年龄段加权后最偏好的搜索词类别
        Map<String, String> mostPreferredWeightedCategoryPerAge = findMostPreferredWeightedCategoryPerAge(searchData, weights);
        System.out.println("\n每个年龄段加权后最偏好的搜索词类别:");
        mostPreferredWeightedCategoryPerAge.forEach((age, category) -> System.out.println(age + ": " + category));
    }

    /**
     * 计算各年龄段对所有搜索词类别的加权偏好总和
     *
     * @param searchData 存储各年龄段搜索词偏好数据的Map，外层键为年龄段，内层键为搜索词类别，值为偏好数值
     * @param weights    存储各搜索词类别权重的Map，键为搜索词类别，值为权重数值
     * @return 一个Map，键为年龄段，值为该年龄段对所有搜索词类别的加权偏好总和
     */
    private static Map<String, Double> calculateWeightedTotalPreferencePerAge(Map<String, Map<String, Double>> searchData, Map<String, Double> weights) {
        Map<String, Double> weightedTotalPreference = new HashMap<>();
        for (Map.Entry<String, Map<String, Double>> entry : searchData.entrySet()) {
            String ageGroup = entry.getKey();
            Map<String, Double> preferences = entry.getValue();
            double total = 0;
            for (Map.Entry<String, Double> preferenceEntry : preferences.entrySet()) {
                String category = preferenceEntry.getKey();
                double preferenceValue = preferenceEntry.getValue();
                double weight = weights.getOrDefault(category, 0.0);
                total += preferenceValue * weight;
            }
            weightedTotalPreference.put(ageGroup, total);
        }
        return weightedTotalPreference;
    }

    /**
     * 找出每个年龄段加权后最偏好的搜索词类别
     *
     * @param searchData 存储各年龄段搜索词偏好数据的Map，外层键为年龄段，内层键为搜索词类别，值为偏好数值
     * @param weights    存储各搜索词类别权重的Map，键为搜索词类别，值为权重数值
     * @return 一个Map，键为年龄段，值为该年龄段加权后最偏好的搜索词类别
     */
    private static Map<String, String> findMostPreferredWeightedCategoryPerAge(Map<String, Map<String, Double>> searchData, Map<String, Double> weights) {
        Map<String, String> mostPreferredWeighted = new HashMap<>();
        for (Map.Entry<String, Map<String, Double>> entry : searchData.entrySet()) {
            String ageGroup = entry.getKey();
            Map<String, Double> preferences = entry.getValue();
            String maxPreferenceCategory = null;
            double maxWeightedValue = Double.MIN_VALUE;
            for (Map.Entry<String, Double> preferenceEntry : preferences.entrySet()) {
                String category = preferenceEntry.getKey();
                double preferenceValue = preferenceEntry.getValue();
                double weight = weights.getOrDefault(category, 0.0);
                double weightedValue = preferenceValue * weight;
                if (weightedValue > maxWeightedValue) {
                    maxWeightedValue = weightedValue;
                    maxPreferenceCategory = category;
                }
            }
            mostPreferredWeighted.put(ageGroup, maxPreferenceCategory);
        }
        return mostPreferredWeighted;
    }
}