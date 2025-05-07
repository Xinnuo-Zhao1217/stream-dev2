package com.zxn.func;

import com.github.houbb.sensitive.word.api.IWordReplace;
import com.github.houbb.sensitive.word.api.IWordResultHandler;
import com.github.houbb.sensitive.word.bs.SensitiveWordBs;
import com.github.houbb.sensitive.word.support.replace.WordReplaces;

import java.util.List;

/**
 * @Package com.zxn.func.SensitiveWordHelper
 * @Author zhao.xinnuo
 * @Date 2025/5/7 11:12
 * @description: SensitiveWordHelper
 */
public class SensitiveWordHelper {
    //
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
        private static final SensitiveWordBs WORD_BS = SensitiveWordBs.newInstance().enableNumCheck(false).enableEmailCheck(false).enableUrlCheck(false).init();

        private SensitiveWordHelper() {
        }

        public static boolean contains(String target) {
            return WORD_BS.contains(target);
        }

        public static List<String> findAll(String target) {
            return WORD_BS.findAll(target);
        }

        public static String findFirst(String target) {
            return WORD_BS.findFirst(target);
        }

        public static String replace(String target, IWordReplace replace) {
            SensitiveWordBs sensitiveWordBs = SensitiveWordBs.newInstance().wordReplace(replace).init();
            return sensitiveWordBs.replace(target);
        }

        public static String replace(String target, char replaceChar) {
            IWordReplace replace = WordReplaces.chars(replaceChar);
            return replace(target, replace);
        }

        public static String replace(String target) {
            return WORD_BS.replace(target);
        }

        public static <R> List<R> findAll(String target, IWordResultHandler<R> handler) {
            return WORD_BS.findAll(target, handler);
        }

        public static <R> R findFirst(String target, IWordResultHandler<R> handler) {
            return WORD_BS.findFirst(target, handler);
        }
    }

