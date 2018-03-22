# 通达信数据清洗与MongoDB数据库操作


-------------------------------

## * 简介

基本步骤如下：

 - 先从通达信上下载1分钟和5分钟的期货数据，操作：系统-盘后数据下载
 - 然后从通达信导出txt或csv文件到指定目录，操作：系统-数据导出-高级导出
 - 启动MongoDB，然后运行tdx_mongodb-operation.py脚本
 注意：这里默认你已经安装好MongoDB、Studio3T（数据可视化工具），并做好了相关配置

开发环境`Python-v3(3.6)`：

 - pandas==0.20.0
 - numpy==1.13.3+mkl
 - pymongo==3.6.0

## * 数据清洗与数据库操作(`tdx_mongodb-operation.py`)

 - conn_mongodb函数，连接数据库，返回一个collection
 - output_symbol_list函数，返回列表，每个元素包含了数据文件夹下所有txt/csv文件的绝对路径和对应的期货品种
 - gen_data_from_txt函数，将txt/csv文件导入并返回DataFrame类型行情数据
 - transfrom函数，将gen_data_from_txt函数导入的数据中的TradingDay（即夜盘数据属于次日交易日）转为ActionDay（即按照正常时间顺序）
 - cut函数，将5分钟数据中的冗余数据清理掉
 - data_processing函数，数据预处理函数，返回最终处理后的数据
 - extract_info函数，从数据库中抽取特定标签数据用作去重处理
 - insert_to_database函数，第一次创建数据库的时候，直接插入数据，往后的维护，需要去重处理之后再插入新数据
 - multi_thread_run函数，单线程处还是有点慢，多线程同时处理多个数据文件导入，效率会高很多，参数：max_threads_num最大线程数量，
   file_path是通达信txt/csv数据所在文件夹目录

## * 用法

 - 配好运行环境以及安装MongoDB，最好再安装一个MongoDB的可视化管理工具Studio 3T
 - 启动MongoDB，最后运行该py脚本即可，需要设置数据跨度（1分钟还是5分钟）、数据库名称、最大启动线程数量，以及通达信txt/csv数据文件夹路径
 - 最终运行后，在MongoDB上显示如下：
 ![image](https://github.com/DemonDamon/tongdaxin-futures-data-clearing-database-operation/blob/master/data-mongod.jpg)
