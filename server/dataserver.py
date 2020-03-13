from aliyun.log import LogClient
from aliyun.log import GetLogsRequest
import pymysql

class ossdataserver:
    def __init__(self,endpoint,accessKeyId,accessKey,basename,tablename):
        
        self.endpoint = endpoint #http://oss-cn-hangzhou.aliyuncs.com
        # 用户访问秘钥对中的 AccessKeyId。
        self.accessKeyId = accessKeyId
        # 用户访问秘钥对中的 AccessKeySecret。
        self.accessKey = accessKey
        self.basename = basename
        self.tablename = tablename
        self.client = LogClient(self.endpoint, self.accessKeyId, self.accessKey)
    def close(self):
        #self.client.shutdown()
        pass
    # def database(self,basename,tablename):
    #     self.basename=basename
    #     self.tablename=tablename

class mysqlserver:
    def __init__(self,host,port,user,passwd,db):
        self.host=host
        self.port=port 
        self.user=user
        self.passwd=passwd
        self.db=db
        self.conn = pymysql.connect(self.host,port=self.port,user=self.user,passwd=self.passwd,db=self.db)
    def close(self):
        self.conn.close()

        