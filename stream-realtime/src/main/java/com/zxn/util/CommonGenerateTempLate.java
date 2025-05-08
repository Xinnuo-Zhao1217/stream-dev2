package com.zxn.util;


import com.zxn.constant.Constant;

import static com.zxn.util.SiliconFlowApi.generateBadReview;

/**
 * @Package com.stream.utils.CommonGenerateTempLate
 * @Author xinyi.jiao
 * @Date 2025/3/16 19:43
 * @description: TempLate
 */
public class CommonGenerateTempLate {

    private static final String COMMENT_TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";

    private static final String COMMENT = "差评";

    public static String GenerateComment(String comment,String productName){
        if (comment.equals(COMMENT)){
            return generateBadReview(
                    String.format(COMMENT_TEMPLATE,COMMENT, productName, "攻击性拉满,使用脏话"),
                    Constant.TOPIC_API
            );
        }
        return generateBadReview(
                String.format(COMMENT_TEMPLATE,COMMENT, productName,""),
                Constant.TOPIC_API
        );
    }

}
