#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pyspark import SparkContext
from pyspark.sql import HiveContext
import datetime

from algorithms.recommend import recommend


class MarsGoodsRecommend():
    def __init__(self):
        os.system('zip -r algorithms.zip algorithms')
        os.system('chmod +x algorithms.zip')
        self.sc = SparkContext()
        self.sc.addPyFile('yourDir/algorithms.zip')
        self.hiveContext = HiveContext(self.sc)
        self.hiveContext.sql("use goods_db")

    def get_origin_goods_list(self):
        try:
            date_yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
            data_yesterday = self.hiveContext.sql(
                    "select goods_id from your_goods where par = (%s) limit 5" % date_yesterday)

            goods_ids = ''
            for topic_goods in data_yesterday.map(lambda g: (g[0])).collect():
                goods_ids += (str(topic_goods) + ',')
            goods_ids = goods_ids[0:-1]

            origin_goods_info = self.hiveContext.sql(
                    "select id, title, back_cat2, back_cat3 from(select id, title, cat2, cat3, row_number() "
                    "over(partition by id order by title) as row_number from goods where id in (%s))a "
                    "where row_number=1" % goods_ids)
            origin_goods_list = origin_goods_info.map(lambda x: x).filter(lambda g: g[2] != 0).map(
                    lambda g: (g[0], g[1].encode('utf-8'), g[2], g[3])).collect()
        except:
            origin_goods_list = []

        return origin_goods_list

    def get_related_goods(self, input_goods):
        try:
            mars_goods_set = self.hiveContext.sql(
                    "select id,title from goods where cat2=%s and cat3=%s" % (input_goods[2],
                                                                                              input_goods[3]))
            mars_goods_set = mars_goods_set.map(lambda g: (g[0], g[1]))
            related_goods_list = []
            for mars_goods in mars_goods_set.collect():
                related_goods = dict()
                related_goods['id'] = mars_goods[0]
                related_goods['title'] = mars_goods[1].encode('utf-8')
                related_goods_list.append(related_goods)
        except:
            related_goods_list = []
        return related_goods_list

    def prepare_goods_info(self, input_goods_list):
        output_goods_info = []
        for goods in input_goods_list:
            goods_candidates = self.get_related_goods(goods)
            goods_dict = list()
            goods_dict.append(goods[1])
            goods_dict += goods_candidates
            output_goods_info.append(goods_dict)
        return output_goods_info

    def get_recommend_goods(self, input_goods_info_list):
        goods_info_rdd = self.sc.parallelize(input_goods_info_list)
        goods_info_rdd = goods_info_rdd.map(lambda g: (g[0], recommend(g[0], g[1:])))
        results = []
        for result in goods_info_rdd.collect():
            results.append(result)

        return results

    @staticmethod
    def print_results(results):
        for result in results:
            print result[0]
            for item in result[1]:
                print item
            print '\n'


if __name__ == "__main__":
    mars_goods_recommend = MarsGoodsRecommend()
    goods_list = mars_goods_recommend.get_origin_goods_list()
    goods_info_list = mars_goods_recommend.prepare_goods_info(goods_list)
    recommend_results = mars_goods_recommend.get_recommend_goods(goods_info_list)
    MarsGoodsRecommend.print_results(recommend_results)
