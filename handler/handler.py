import datetime
from config.base import conf
import pandas as pd
from helper.helper import ProductHelper,UserHelper
from server.dataserver import ossdataserver,mysqlserver
from config.base import conf,varible
from pandas.tseries.offsets import Day
from time import time
import pickle
import itertools
from surprise import Reader
from surprise import Dataset
from surprise import KNNBaseline

from collections import Counter

class ProductHandler(object):
    """数据处理层"""
    def __init__(self,mysql_server):
        self.producthelper=ProductHelper(mysql_server)

    def product_info(self,num=100000000):
        #评分排序，返回num个
        #res=pd.merge(self.producthelper.get_product_data(), self.producthelper.get_sku_data(), on='product_id', how='left')
        res=self.producthelper.get_product_data()
        return res
    
    def product_weight_sort(self):
        # 商品定权重，排序
        pass

class CollaborateHandler(object):
    """数据处理层"""
    def __init__(self,mysql_server,oss_server):
        self.producthelper=ProductHelper(mysql_server)
        self.userhelper=UserHelper(mysql_server,oss_server)

    def Collaborate_info(self,corr_user,userlist,start_time,end_time,num=100000000):
        #评分排序，返回num个
        #[rating.append(i[0]+' '+i[1]+' '+'0'+' '+'10000000000') for i in itertools.product(userlist,productdata['product_id'])]
        
        rating=[]
        productdata=self.producthelper.get_product_data()
        if userlist==[]:
            orderdata,userlist=self.userhelper.get_order_user_data(start_time,end_time)
        else:
            orderdata=self.userhelper.get_order_data(userlist,start_time,end_time)
        
        behaviordata=self.userhelper.get_behavior_data(userlist,start_time,end_time)
        
        recommend={}
        for i,user in enumerate(userlist):
            product={}
            temp_order=orderdata[orderdata['user_id']==int(user.replace('"',''))]
            temp_beha=behaviordata[behaviordata['user_id']==int(user.replace('"',''))]
            A=dict(temp_order['product_id'].iloc[0]) if len(temp_order['product_id'])!=0 else {}
            B=dict(temp_beha['product_id'].iloc[0]) if len(temp_beha['product_id'])!=0 else {}
            for key in list(set(A) | set(B)):
                if A.get(key) and B.get(key):
                    product.update({key: A.get(key)*5 + B.get(key)})
                else:
                    product.update({key: B.get(key) or A.get(key)*5})
            recommend[user.replace('"','')]=product
        return recommend


        '''
        # surprise推荐
        id_name_dic={}
        name_id_dic={}
        for i,pro in enumerate(productdata['product_id']):
            id_name_dic[pro]=productdata['product_title'][productdata['product_title'].index[i]]
            name_id_dic[productdata['product_title'][productdata['product_title'].index[i]]]=pro

        for i,bdata in enumerate(behaviordata['product_id']):
            for k in bdata.keys():
                rating.append(behaviordata['user_id'][behaviordata['user_id'].index[i]]+' '+k+' '+bdata[k]+' '+'10000000000')
                #score.append(behaviordata['user_id'][behaviordata['user_id'].index[i]]+' '+k)

        for i,bdata in enumerate(orderdata['product_id']):
            for k in bdata.keys():
                rating.append(orderdata['user_id'][orderdata['user_id'].index[i]]+' '+k+' '+bdata[k]+' '+'10000000000')
                #score.append(orderdata['user_id'][orderdata['user_id'].index[i]]+' '+k)
        
        # 只有正面的数据，没有负面数据
        # for pid in productdata['product_id']:
        #     pass


        user=[]
        item=[]
        rating=[]
        timestamp=[]

        ##告诉surprise你的数据形式
        reader = Reader(line_format='user item rating timestamp',sep=' ')

        #建立可用于surprise的数据集
        data = Dataset.load_from_df(rating,reader=reader)
        print("构建数据集...")
        trainset = data.build_full_trainset()
        sim_options = {'name': 'pearson_baseline', 'user_based': False}
        print("开始训练模型...")
        #sim_options = {'user_based': False}
        #algo = KNNBaseline(sim_options=sim_options)
        algo = KNNBaseline()
        algo.train(trainset)

        current_playlist = name_id_dic.keys()[39]
        print(current_playlist)

        # 取出近邻
        playlist_id = name_id_dic[current_playlist]
        print(playlist_id)
        playlist_inner_id = algo.trainset.to_inner_uid(playlist_id)
        print(playlist_inner_id)

        playlist_neighbors = algo.get_neighbors(playlist_inner_id, k=corr_user)
        '''


        return orderdata


class UserHandler(object):
    """数据处理层"""
    def __init__(self,mysql_server,oss_server):
        self.userhelper=UserHelper(mysql_server,oss_server)

    def user_info(self,userlist,start_time,end_time):
        data=self.userhelper.get_order_data(userlist)
        data=pd.merge(data,self.userhelper.get_behavior_data(userlist,start_time,end_time),on='user_id',how='left')
        return data
    
    def group_info(self,userlist,start_time,end_time):
        # 需要把userid连成成一个组id，算作一个用户
        self.userhelper.get_order_data(userlist)
        self.userhelper.get_behavior_data(userlist,start_time,end_time)
        return userlist
    
    def all_user_info(self,start_time,end_time):
        orderdata,userlist=self.userhelper.get_order_user_data(start_time,end_time)
        self.userhelper.get_behavior_data(userlist,start_time,end_time)
        return userlist

    
if __name__ == "__main__":
    mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))
    oss_server = ossdataserver(conf.get('access','endpoint'),conf.get('access','accessKeyId'),conf.get('access','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))
    handler = UserHandler(mysql_server,oss_server)
    
    userlist=['"11648742"','"14883478"','"15895419"','"5872187"','"16607035"']
    
    # handler.get_order_data(userlist)
    # handler.get_sku_data()
    # handler.get_product_data()

    # 构造昨天的时间
    # #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')
    start_time = '2019-08-01'#(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')#input(u'请输入要获取订单的开始时间：如（2019-01-01）')
    # #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')
    end_time = '2019-09-01' #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')#input(u'请输入结束时间：如（2019-01-07）')

    handler.user_info(userlist,start_time,end_time)
