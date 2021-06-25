from pyspark.context import SparkContext, SparkConf
conf = SparkConf().setAppName('wiki').setMaster('local[*]')
sc = SparkContext(conf=conf)
import re
import os.path

def links(entity):
    wikilink_1 = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]')
    result = re.findall(wikilink_1, entity)
    return list(set(result))

def jaccard_similarity(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    intersection = len(set.intersection(set1, set2))
    union = len(set.union(set1, set2))
    return intersection/union

rdd = sc.wholeTextFiles("../data/*/*")
# first map all text files to (article_name, (timestamp, [link_1, link_2, ..., link_n]))
# then reduce by article name resulting in (article_name, list_of_timestamp_link_tuples)
ls = rdd.map(lambda x: (os.path.dirname(x[0]), [(os.path.basename(x[0]), links(x[1]))])) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0], x[1][-1:])) # last 5 versions only
ls = sc.parallelize(ls.take(10))
ls = ls.cartesian(ls)
# first map to (title_1, title_2), ([links_1], [links_2])
# then compute jaccard similarity between two articles
ls = ls \
       .map(lambda row: ((row[0][0], row[1][0]), (row[0][1], row[1][1]))) \
       .map(lambda row: (row[0], jaccard_similarity(row[1][0][0][1], row[1][1][0][1])))
ls.saveAsTextFile("res_cartesian.csv")


