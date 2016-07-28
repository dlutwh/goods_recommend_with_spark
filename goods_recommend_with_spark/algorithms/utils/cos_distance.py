#!/usr/bin/env python
# -*- coding: utf-8 -*-
import jieba
import jieba.posseg
import jieba.analyse

topK = 5


def cos_distance(string_a, string_b):
    # print "string_a:", string_a
    # print "string_b:", string_b

    # 核心词提取,取名词,人名,地名
    words_a = list(jieba.analyse.textrank(string_a, topK=topK, withWeight=False, allowPOS=('ns', 'n', 'nr')))
    words_b = list(jieba.analyse.textrank(string_b, topK=topK, withWeight=False, allowPOS=('ns', 'n', 'nr')))

    if 0 == len(words_a) or 0 == len(words_b):
        return 0

    words_all = list(set(words_a).union(set(words_b)))

    words_a_trans = [int(x in words_a) for x in words_all]
    words_b_trans = [int(x in words_b) for x in words_all]

    return distance(words_a_trans, words_b_trans)


def distance(vector1, vector2):
    dot_product = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for a, b in zip(vector1, vector2):
        dot_product += a * b
        norm_a += a ** 2
        norm_b += b ** 2
    if norm_a == 0.0 or norm_b == 0.0:
        return 0
    else:
        return dot_product / ((norm_a * norm_b) ** 0.5)


if __name__ == "__main__":
    string_A = "良品铺子】迷你烤香肠 290g*2 香辣味（非偏远地区包邮）"
    string_B = "良品铺子】新奥尔良风味小鸡腿158g"

    print cos_distance(string_A, string_B)

