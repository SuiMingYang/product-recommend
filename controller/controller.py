import datetime
from config.base import conf
import pandas as pd
from helper.helper import ProductHelper,UserHelper
from server.dataserver import ossdataserver,mysqlserver
from config.base import conf,varible
from pandas.tseries.offsets import Day
from tools.cmsapi import get_token
from time import time
import requests
import re
from collections import Counter
from PIL import Image
from skimage import io
import pytesseract
import requests as req
from io import BytesIO
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from handler.handler import UserHandler,ProductHandler,CollaborateHandler
from helper.usercf_recommend import UserCf

class CommendController(object):
    """业务逻辑层"""
    def __init__(self):
        pass

    # 用户协同推荐
    def usercf_commend(self,start_time,end_time,userlist=[],corr_user=10,num=50):
        mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))      
        oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))
        #协同过滤，扫全量用户，做分组推荐
        collaborate=CollaborateHandler(mysql_server,oss_server)
        coll_data=collaborate.Collaborate_info(corr_user,userlist,start_time,end_time)
        usercf=UserCf(coll_data)
        recommandList=usercf.recomand('1475735', 2)
        print(recommandList)
        userList=usercf.nearstUser('1475735', 2)
        print(userList)

        # id链接商品标题
        # coll_data=

        return coll_data

    # 商品协同推荐
    def itemcf_commend(self,corr_product=10,num=50):
        #备用
        pass

    # 标签推荐
    def tag_commend(self,start_time,end_time,num=10):
        mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))      
        oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))
        
        Collaborate=CollaborateHandler(mysql_server,oss_server)
        #productinfo=Collaborate.Collaborate_info(start_time,end_time)
        mysql_server.close()
        
        return ""

    # 单用户推荐
    def single_commend(self,userlist,start_time,end_time,num=50):
        mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))      
        oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))

        prohandler=ProductHandler(mysql_server)
        handler=UserHandler(mysql_server,oss_server)
        order=handler.user_info(userlist,start_time,end_time)
        
        product=prohandler.product_info()
        mysql_server.close()
        return product

    # 群组推荐
    def group_commend(self,userlist,start_time,end_time,num=50):
        # 一个分组推送一次50个
        mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))      
        oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))

        prohandler=ProductHandler(mysql_server)
        handler=UserHandler(mysql_server,oss_server)
        mysql_server.close()
        return num

    # 大众推荐
    def cold_commend(self,num=50):
        # 冷启动，新用户
        mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))
        
        prohandler=ProductHandler(mysql_server)
        pro=prohandler.product_info()
        mysql_server.close()
        # xk_pro=pro[pro['mp_alias']=='gh212565199e78']
        # qs_pro=pro[pro['mp_alias']=='']
        # import jieba
        # jieba.load_userdict("./resources/dict/expendword.txt")
        # xk_str=' '.join(xk_pro['product_title'])
        # text_word=Counter(jieba.lcut(xk_str))
        # xk_k=[]
        # xk_v=[]
        # for k in text_word.keys():
        #     xk_k.append(k)
        # for v in text_word.values():
        #     xk_v.append(v)
        
        # xk_obj={
        #     'word':xk_k,
        #     'count':xk_v
        # }
        # xk_pd=pd.DataFrame(xk_obj,index=None)
        # xk_pd.to_csv('qingshe_word.csv',index=None)
        
        ss = StandardScaler()

        std_sort = ss.fit_transform([[i] for i in pro['sort']])
        pro['sort']=[i[0] for i in std_sort]
        std_buynum=ss.fit_transform([[i] for i in pro['product_buy_num']])
        pro['product_buy_num']=[i[0] for i in std_buynum]
        std_collnum=ss.fit_transform([[i] for i in pro['product_collect_num']])
        pro['product_collect_num']=[i[0] for i in std_collnum]
        std_safe=ss.fit_transform([[i] for i in pro['safeguard_radtio']])
        pro['safeguard_radtio']=[i[0] for i in std_safe]
        std_pv=ss.fit_transform([[i] for i in pro['pv']])
        pro['pv']=[i[0] for i in std_pv]

        pro['score']=pro['sort']*0.1+pro['product_buy_num']*3+pro['product_collect_num']*2+pro['pv']*1-pro['safeguard_radtio']*0.5

        pro.sort_values(by="score",ascending=False,inplace=True)
        return pro.iloc[0:num]
        
        # #标签推荐
        # qs_id=[]
        # qs_name=[]
        # qs_key=[]
        # xk_id=[]
        # xk_name=[]
        # xk_key=[]
        # qs=pro[pro['mp_alias']=='gh212565199e78']
        # import jieba
        # import jieba.posseg as pseg # 词性标注
        # #from jieba.analyse import ChineseAnalyzer
        # from jieba.analyse import extract_tags
        # jieba.load_userdict("./resources/dict/expendword.txt")
        # for i,item in enumerate(qs['product_title']):
        #     #seg_list=#,cut_all=False)
        #     qs_id.append(qs['product_id'][qs['product_id'].index[i]])
        #     qs_name.append(item)
        #     seg_obj={}
        #     seg_list=jieba.lcut(item)#pseg.cut(item)#,cut_all=False)
        #     qs_key+=seg_list
        # print(Counter(qs_key))
        # key=[]
        # val=[]
        # for io in sorted(Counter(qs_key).items(), key = lambda kv:(kv[1], kv[0]),reverse=True):
        #     (i,o)=io
        #     key.append(i)
        #     val.append(o)
        # xk_obj={
        #     'key':key,
        #     'val':val
        # }
        # xk_pd=pd.DataFrame(xk_obj,index=None)
        # xk_pd.to_csv('./kv.csv',index=None)
        # qs_obj={
        #     'id':qs_id,
        #     'name':qs_name,
        #     'key':qs_key
        # }
        # qs_pd=pd.DataFrame(qs_obj,index=None)
        # qs_pd.to_csv('./qs_key.csv',index=None)

        # for i,item in enumerate(xk['product_title']):
        #     #seg_list=pseg.cut(item)#,cut_all=False)
        #     xk_id.append(xk['product_id'][xk['product_id'].index[i]])
        #     seg_obj={}
        #     #item=''.join(re.findall(u'[a-zA-Z\d\u4e00-\u9fff]+', item))
        #     seg_list=pseg.cut(item)#,cut_all=False)
        #     for word in seg_list:
        #         if word.flag in list(seg_obj.keys()):
        #             seg_obj[word.flag].append(word.word)
        #         else:
        #             seg_obj[word.flag]=[]
        #             seg_obj[word.flag].append(word.word)
        #     xk_key.append(seg_obj)
        # xk_obj={
        #     'id':xk_id,
        #     'name':xk_name,
        #     'key':xk_key
        # }
        # xk_pd=pd.DataFrame(xk_obj,index=None)
        # xk_pd.to_csv('./xk_key.csv',index=None)


        # for i,pic in enumerate(pro['details']):
        #     res=requests.get('https://html.weidiango.com/{0}'.format(pic),{},headers={'token':get_token()})
        #     print(pro['product_id'][pro['product_id'].index[i]])
        #     for detail in re.findall(r'data-src="([\w]+)"',res.text,re.M|re.I):
        #         #image = io.imread('https://img.weidiango.com/{0}?imageMogr2/gravity/center/thumbnail/828x/format/jpeg'.format(detail))
                
        #         url = requests.get('https://img.weidiango.com/{0}?imageMogr2/gravity/center/thumbnail/828x/format/jpeg'.format(detail),{},headers={'token':get_token()})
        #         image = Image.open(BytesIO(url.content))
        #         #image = Image.open('https://img.weidiango.com/{0}?imageMogr2/gravity/center/thumbnail/828x/format/jpeg'.format(detail))   # 打开图片
        #         text = pytesseract.image_to_string(image,lang='chi_sim').replace(' ','')  #使用简体中文解析图片
        #         print(text)
        
        return prohandler.product_info(num)



if __name__ == "__main__":
    st=datetime.datetime.now()
    mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))
    oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))
    handler=UserHandler(mysql_server,oss_server)
    prohandler=ProductHandler(mysql_server)
    
    userlist=['"11648742"','"14883478"','"15895419"','"5872187"','"16607035"']

    # 构造昨天的时间
    # #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')
    start_time = '2019-08-01'#(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')#input(u'请输入要获取订单的开始时间：如（2019-01-01）')
    # #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')
    end_time = '2019-09-01' #(datetime.datetime.now() -1*Day()).strftime('%Y-%m-%d')#input(u'请输入结束时间：如（2019-01-07）')

    #handler.user_info(userlist,start_time,end_time)
    prohandler.product_info()
    et=datetime.datetime.now()
    print('执行用时：',et-st)