package com.zxn.func;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.zxn.func.JudgmentFunc
 * @Author zhao.xinnuo
 * @Date 2025/5/15 10:04
 * @description: JudgmentFunc
 */
public class JudgmentFunc {
    public static String ageJudgment(Integer age) {
        if (age != null && age >= 18 && age <= 24) {
            return "18-24";
        } else if (age >= 25 && age <= 29) {
            return "25-29";
        } else if (age >= 30 && age <= 34) {
            return "30-34";
        } else if (age >= 35 && age <= 39) {
            return "35-39";
        } else if (age >= 40 && age <= 49) {
            return "40-49";
        } else if (age >= 50) {
            return "50+";
        }
        return "0-17";

    }

    public static String EraJudgment(String  birthDate) {
        // 1. 解析日期
        LocalDate date = LocalDate.parse(birthDate, DateTimeFormatter.ISO_DATE);

        // 2. 获取年份
        int year = date.getYear();

        // 3. 计算年代（每10年为一个年代）
        int decadeStart = (year / 10) * 10; // 例如：1985 → 1980

        // 4. 格式化输出
        return String.format("%d0年代", decadeStart / 10);
    }

    public static String ConstellationJudgment(String  birthDate) {
        LocalDate date = LocalDate.parse(birthDate, DateTimeFormatter.ISO_DATE);
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

        // 星座判断逻辑
        switch (month) {
            case 1:  // 1月：摩羯座（1/1-1/19）或 水瓶座（1/20-1/31）
                return (day <= 19) ? "摩羯座" : "水瓶座";
            case 2:  // 2月：水瓶座（2/1-2/18）或 双鱼座（2/19-2/29）
                return (day <= 18) ? "水瓶座" : "双鱼座";
            case 3:  // 3月：双鱼座（3/1-3/20）或 白羊座（3/21-3/31）
                return (day <= 20) ? "双鱼座" : "白羊座";
            case 4:  // 4月：白羊座（4/1-4/19）或 金牛座（4/20-4/30）
                return (day <= 19) ? "白羊座" : "金牛座";
            case 5:  // 5月：金牛座（5/1-5/20）或 双子座（5/21-5/31）
                return (day <= 20) ? "金牛座" : "双子座";
            case 6:  // 6月：双子座（6/1-6/20）或 巨蟹座（6/21-6/30）
                return (day <= 20) ? "双子座" : "巨蟹座";
            case 7:  // 7月：巨蟹座（7/1-7/22）或 狮子座（7/23-7/31）
                return (day <= 22) ? "巨蟹座" : "狮子座";
            case 8:  // 8月：狮子座（8/1-8/22）或 处女座（8/23-8/31）
                return (day <= 22) ? "狮子座" : "处女座";
            case 9:  // 9月：处女座（9/1-9/22）或 天秤座（9/23-9/30）
                return (day <= 22) ? "处女座" : "天秤座";
            case 10: // 10月：天秤座（10/1-10/22）或 天蝎座（10/23-10/31）
                return (day <= 22) ? "天秤座" : "天蝎座";
            case 11: // 11月：天蝎座（11/1-11/21）或 射手座（11/22-11/30）
                return (day <= 21) ? "天蝎座" : "射手座";
            case 12: // 12月：射手座（12/1-12/21）或 摩羯座（12/22-12/31）
                return (day <= 21) ? "射手座" : "摩羯座";
            default:
                throw new IllegalArgumentException("无效月份");
        }
    }

    /**
     * 根据时间戳判断时间段分类
     * @param timestamp 毫秒时间戳
     * @return 时间段名称（中文）
     */
    public static String timePeriodJudgment(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("无效时间戳");
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.of("Asia/Shanghai")
        );

        int hour = dateTime.getHour();

        if (hour >= 0 && hour < 6) return "凌晨";
        if (hour < 9) return "早晨";
        if (hour < 12) return "上午";
        if (hour < 14) return "中午";
        if (hour < 18) return "下午";
        if (hour < 22) return "晚上";
        return "夜间";
    }

    /**
     * 根据商品金额进行分类判断
     * @param totalAmount 商品金额（支持小数）
     * @return 价格分类标签
     */
    public static String priceCategoryJudgment(double totalAmount) {
        if (totalAmount < 0) {
            throw new IllegalArgumentException("金额不能为负数: " + totalAmount);
        }
        // 边界值精确处理
        if (totalAmount < 1000) {
            return "低价商品";
        } else if (totalAmount >= 1000 && totalAmount <= 4000) {
            return "中价商品";
        } else if (totalAmount > 4000) {
            return "高价商品";
        }
        return "无效金额";
    }



}
