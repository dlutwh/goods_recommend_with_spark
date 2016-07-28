#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils.cos_distance import cos_distance

RESULT_NUM = 10


def recommend(origin, item_list):
    distance = {}
    for goods in item_list:
        dis = cos_distance(origin, goods['title'])
        idx = str(goods['id']) + '_' + goods['title']
        if dis != 0:
            distance[idx] = dis
    sort_dis = sorted(distance.iteritems(), key=lambda d: d[1], reverse=True)

    result_list = []
    result_num = min(RESULT_NUM, len(sort_dis))
    for i in range(result_num):
        result_list.append(sort_dis[i][0])
    result_list = list(set(result_list))

    return result_list


if __name__ == "__main__":
    print "Hello"
