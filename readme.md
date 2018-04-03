# 基于神经网络的基本搜索引擎实现

开发环境： python3.6, maxOS, Pycharm2017

外部依赖：
   > - BeautifulSoup4
    

实现及原理简介：
    首先，我们需要利用python爬虫爬取尽可能多的网页，因为该项目的关注于算法实现，所以我只爬取了ChinaDaily这一新闻网站，根据
    爬取的内容建立数据库，大概建立了5个表，把关键字，位置，url等相关信息存储进去。
    此时，我们可以使用进行简单的查询了，对于查询的字符串我们要进行处理，如去除无意词，分词，然后分别对它们查询，为了返回最佳的结果，我们要对返回的一系列结果
    评分，这也是搜索引擎算法的关键部分。
    方法一：基于词频进行排序，含有该关键词越多的页面，权重越大
    方法二：基于关键词在文档中的位置，位置越靠前，权重越大
    

    
    