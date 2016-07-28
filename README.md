# goods_recommend_with_spark
This project is a simply goods recommend system with spark written in python

项目依赖：jieba 分词

项目运行：
  集群：spark-submit main.py
  单机：spark-submit --master local[*] main.py
  
简单说明：
  本项目用于通过商品标题信息寻找与目标商品最相似的商品。
  算法中对商品标题采用 jieba 分词并提取其中的核心名词，形成商品的特征向量。
  商品相似度计算采用的是向量余弦距离。
