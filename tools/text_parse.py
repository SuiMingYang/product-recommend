#coding:utf-8
import re
import pandas as pd
import numpy as np
import json
import jieba
import jieba.posseg as pseg # 词性标注
#from jieba.analyse import ChineseAnalyzer
from jieba.analyse import extract_tags

class TextParse(object):
    def __init__(self):
        pass
    
    @staticmethod
    def title2arr(title):
        return jieba.lcut(title)