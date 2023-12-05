# %%
from math import ceil
from nltk.tokenize import word_tokenize
import argparse
from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, Row, DataFrame
import pyspark.sql.functions as F
import random
import ast

DATA_PATH = './data/body.csv'
DATA_LEN = 19043


def shingles(rows: RDD, k: int) -> RDD:
    def parse_shingles(row):
        res = []
        tokens = str(row.asDict()['body']).split(' ')
        for i in range(len(tokens) - k + 1):
            res.append(
                (tuple([tokens[j] for j in range(i, i + k)]), (row.asDict()['id'], )))
        return res

    def parse_list(tup):
        res = [0] * DATA_LEN
        for t in tup:
            res[int(t)] = 1
        return res

    return rows.flatMap(lambda row: parse_shingles(row))\
        .reduceByKey(lambda id1, id2: id1 + id2)\
        .mapValues(lambda v: parse_list(v))


def minhash(rows: RDD, h: int):
    def parse_sig(row):
        return [-1 if i == 0 else row[1][1] for i in row[0][1]]

    def merge_sig(sig1, sig2):
        res = []
        for i, j in zip(sig1, sig2):
            if i == -1:
                res.append(j)
            elif j == -1:
                res.append(i)
            elif i < j:
                res.append(i)
            else:
                res.append(j)
        return res

    def get_non_factor(n: int, m=50) -> list:
        res = []
        for i in range(1, m + 1):
            if n % i != 0:
                res.append(i)
        return res

    cnt = rows.count()
    sig_lst = []
    for i in range(h):
        # # cnt = 572322  # ! magic number for 2 shingles
        a = random.choice(get_non_factor(cnt))
        b = ceil(random.random() * 1000)
        print(cnt, a, b)
        sig_lst.append(rows.zipWithIndex()
                       .mapValues(lambda id: (id, (a * id + b) % cnt))
                       .sortBy(lambda row: row[1][1])
                       .map(lambda row: parse_sig(row))
                       .reduce(lambda sig1, sig2: merge_sig(sig1, sig2)))

    return sig_lst


def lsh(mh_rdd: RDD):

    def parse_bucket(row):
        b = [[], [], [], []]
        for i in row:
            if i <= 2500:
                b[0].append(i)
            elif i <= 5000:
                b[1].append(i)
            elif i <= 7500:
                b[2].append(i)
            else:
                b[3].append(i)
        return b

    def merge_bucket(b1, b2):
        for i in range(len(b1)):
            b1[i].extend(b2[i])
        return b1
    return mh_rdd.map(lambda row: parse_bucket(row))\
        .reduce(lambda x1, x2: merge_bucket(x1, x2))


# %%
'''
ax+b mod $len
'''
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', default=2, type=int)
    parser.add_argument('-H', default=1, type=int)
    args = parser.parse_args()
    conf = SparkConf()
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName('LSH')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    rows = spark.read.csv(DATA_PATH, sep=',', header=True).rdd
    s = shingles(rows, args.k)
    s.map(lambda r: Row(str(r[0]), r[1])).toDF().show(n=10)
    mh = minhash(s, args.H)
    print(mh)
    # mh_rdd = sc.parallelize(mh, 20)
    # print(lsh(mh_rdd))
# %%
