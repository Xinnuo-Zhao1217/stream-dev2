package com.zxn.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @Package com.zxn.func.PublicFilterBloomFunc
 * @Author zhao.xinnuo
 * @Date 2025/5/15 10:03
 * @description: PublicFilterBloomFunc
 */
public class PublicFilterBloomFunc extends RichFilterFunction<JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(PublicFilterBloomFunc.class);

    //预期插入的元素数量
    private final int expectedInsertions; // 预期插入的元素数量
    //期望的误判率
    private final double falsePositiveRate; // 允许的误判率
    private final String sortKey1; // 允许的误判率
    private final String sortKey2; // 允许的误判率
    private transient ValueState<byte[]> bloomState; // Flink状态存储，用于保存布隆过滤器的位数组

    /**
     * 构造函数，初始化布隆过滤器参数
     * @param expectedInsertions 预期插入的元素数量
     * @param falsePositiveRate  允许的误判率，如0.01表示1%的误判率
     */
    public PublicFilterBloomFunc(int expectedInsertions, double falsePositiveRate,String sortKey1,String sortKey2) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
        this.sortKey1 = sortKey1;
        this.sortKey2 = sortKey2;
    }

    // 初始化阶段创建并获取 Flink 的状态描述符
    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState",
                BytePrimitiveArraySerializer.INSTANCE
        );

        bloomState = getRuntimeContext().getState(descriptor);
    }

    /**
     * 核心过滤逻辑：判断元素是否重复
     * @param value 输入的JSON对象
     * @return true表示保留数据（不重复），false表示过滤数据（可能重复）
     */
    @Override
    public boolean filter(JSONObject value) throws Exception {
        // 1. 从JSON对象中提取唯一标识：order_id和时间戳的组合
        if (value == null) {
            // 可选：记录日志或者直接过滤掉
            return false; // 过滤掉 null 数据
        }
        long tsMs = value.getLong(sortKey2);
        if (value.getJSONObject("after" ) != null){
            String unique = value.getJSONObject("after").getString(sortKey1);
            String compositeKey = unique + "_" + tsMs;

            // 读取布隆过滤器状态
            byte[] bitArray = bloomState.value();
            if (bitArray == null) {
                // 初始化位数组，计算所需的字节数
                bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
            }
            // 3. 使用双哈希技术生成两个哈希值
            boolean mightContain = true;
            //右移
            int hash1 = hash(compositeKey);
            int hash2 = hash1 >>> 16;

            // 4. 计算所有哈希位置并检查是否存在
            for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
                // 生成第i个哈希值 确保为正数
                int combinedHash = hash1 + (i * hash2);
                if (combinedHash < 0) combinedHash = ~combinedHash;
                int pos = combinedHash % (bitArray.length * 8);

                // 计算在位数组中的位置
                int bytePos = pos / 8; // 字节位置
                int bitPos = pos % 8; // 位位置
                byte current = bitArray[bytePos];

                // 检查对应位是否为1
                if ((current & (1 << bitPos)) == 0) {
                    // 如果任何一位为0，则元素肯定不存在
                    mightContain = false;
                    // 将对应位设置为1
                    bitArray[bytePos] = (byte) (current | (1 << bitPos));
                }
            }

            // 5. 根据检查结果决定是否保留数据
            // 如果是新数据，更新状态并保留
            if (!mightContain) {
                // 新元素：更新状态并保留
                bloomState.update(bitArray);
                return true;
            }

            // 可能重复的数据，过滤
            logger.warn("check duplicate data : {}", value);
            return false;

        }else {
            //如果不是嵌套 after 层 则直接获取
            String unique = value.getString(sortKey1);
            String compositeKey = unique + "_" + tsMs;

            // 读取布隆过滤器状态
            byte[] bitArray = bloomState.value();
            if (bitArray == null) {
                // 初始化位数组，计算所需的字节数
                bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
            }
            // 3. 使用双哈希技术生成两个哈希值
            boolean mightContain = true;
            //右移
            int hash1 = hash(compositeKey);
            int hash2 = hash1 >>> 16;

            // 4. 计算所有哈希位置并检查是否存在
            for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
                // 生成第i个哈希值 确保为正数
                int combinedHash = hash1 + (i * hash2);
                if (combinedHash < 0) combinedHash = ~combinedHash;
                int pos = combinedHash % (bitArray.length * 8);

                // 计算在位数组中的位置
                int bytePos = pos / 8; // 字节位置
                int bitPos = pos % 8; // 位位置
                byte current = bitArray[bytePos];

                // 检查对应位是否为1
                if ((current & (1 << bitPos)) == 0) {
                    // 如果任何一位为0，则元素肯定不存在
                    mightContain = false;
                    // 将对应位设置为1
                    bitArray[bytePos] = (byte) (current | (1 << bitPos));
                }
            }

            // 5. 根据检查结果决定是否保留数据
            // 如果是新数据，更新状态并保留
            if (!mightContain) {
                // 新元素：更新状态并保留
                bloomState.update(bitArray);
                return true;
            }

            // 可能重复的数据，过滤
            logger.warn("check duplicate data : {}", value);
            return false;
        }

    }

    /**
     * 计算布隆过滤器的最佳哈希函数数量
     * @param n 预期插入的元素数量
     * @param m 位数组的大小（以位为单位）
     * @return 最佳哈希函数数量
     */
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * 计算布隆过滤器的最佳位数组大小
     * @param n 预期插入的元素数量
     * @param p 允许的误判率
     * @return 最佳位数组大小（以位为单位）
     */
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 使用MurmurHash3算法生成哈希值
     * @param key 输入的键值
     * @return 32位哈希值
     */
    private int hash(String key) {
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }
}

