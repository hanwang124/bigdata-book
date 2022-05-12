#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pyhdfs

if __name__=="__main__":
    fs=pyhdfs.HdfsClient(hosts="localhost:50070",user_name="root")
    home_dir = fs.get_home_directory()
    print(home_dir)
    
    #fs.copy_from_local("./data/booktuijian.csv","/data/booktuijian.csv")
    fs.copy_from_local("./data/BX-Book-Ratings.csv","/data/BX-Book-Ratings.csv")
    fs.copy_from_local("./data/BX-Books.csv","/data/BX-Books.csv")
    fs.copy_from_local("./data/BX-Users.csv","/data/BX-Users.csv")


# #!/usr/bin/env python
# # -*- coding:utf-8 -*-
# """
# 安装依赖包
#     pip install hdfs
#     pip install thrift
# """
#
# from hdfs.client import Client
# from hdfs import InsecureClient
#
# # 删除hdfs文件
# def delete_hdfs_file(client, hdfs_path):
#     client.delete(hdfs_path)
#
#
# # 上传文件到hdfs
# def put_to_hdfs(client, local_path, hdfs_path):
#     client.upload(hdfs_path, local_path, cleanup=True)
#
#
# def read_hdfs_file(client, hdfs_path):
#     # 读文件内容
#     with client.read(hdfs_path, encoding='utf-8') as reader:
#         # 解码（源数据为`b''`样式）
#         print(reader.read())
#         # out = reader.read()#.decode("utf-8")
#         # #    测试输出
#         # print(out)
#
#
# def main():
#     client = Client("http://8.130.15.118:50070",root='/')
#     print(client.status('.'))
#     print(client.list('/data'))
#     client.upload("/data", "./data/booktuijian.csv")
#
#     client.download('/data/VERSION', './data')
#     # delete_hdfs_file(client, '/tmp/hdfs_hbase_operation.py')
#     # put_to_hdfs(client, "D:\workspace\python_workspace\student_dianping\com\example\hdfs_hbase_operation.py", '/tmp')
#
#     #put_to_hdfs(client,'./data/booktuijian.csv','/data/booktuijian.csv')
#     read_hdfs_file(client, '/data/VERSION')
#
#
# if __name__ == '__main__':
#     main()
#
