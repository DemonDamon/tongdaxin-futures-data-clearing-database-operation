import os, warnings, threading
import numpy as np
import pandas as pd
from sys import path
from pymongo import MongoClient
from concurrent import futures

class tdx_mongodb_operation(object):
	def __init__(self,*arg,**kwarg):
		self._IP = arg[0]
		self._PORT = arg[1]
		self._dataFrame = kwarg["dataframe"]
		self._databaseName = kwarg["database"]

	def conn_mongodb(self,collectionName):
		self._Conn = MongoClient(self._IP, self._PORT)
		self._mydb = self._Conn[self._databaseName]
		collection = self._mydb.get_collection(collectionName)
		return collection

	def output_symbol_list(self,file_path):
		symbol_info_list = []
		for root, dirs, files in os.walk(file_path):
			for file in files:
				if os.path.splitext(file)[1] == '.txt':
					symbol_info_list.append([os.path.join(root, file),file.replace('L8.txt','')])
		return symbol_info_list

	def gen_data_from_txt(self,symbol_path):
		raw_data = []; Date = []; Time = []
		Open = []; High = []; Low = []; Close = []
		Volumn = []; OpenInterest = []
		for line in open(symbol_path):
			row = line.split(',')			        	
			if row[0].isdigit():
				Date.append(row[0])
				Time.append(row[1])
				Open.append(row[2])
				High.append(row[3])
				Low.append(row[4])
				Close.append(row[5])
				Volumn.append(row[6])
				OpenInterest.append(row[7])			
		raw_data = pd.DataFrame({'Date' : np.int64(Date),
									'Time' : np.int32(Time),
									'Open' : np.double(Open),
									'High' : np.double(High),
									'Low' : np.double(Low),
									'Close' : np.double(Close),
									'Volumn' : np.int64(Volumn),
									'OpenInterest' : np.int64(OpenInterest)}, 
									columns=['Date','Time','Open',
											'High','Low','Close',
											'Volumn','OpenInterest'])
		return raw_data

	def transfrom(self,latest_raw_data):
		if self._dataFrame == 5:
			print('  data frame: 5 mins ')
			i = 0
			while i < len(latest_raw_data):
				if latest_raw_data.iloc[i,1] == 1500:
					k = i + 1
					t = []
					date = latest_raw_data.iloc[i,0]
					while k < len(latest_raw_data) and latest_raw_data.iloc[k,1] != 905:
						if latest_raw_data.iloc[k,1] >= 2105 and latest_raw_data.iloc[k,1] <= 2355:
							t.append(k)
						k += 1
					if len(t) != 0:
						for j in range(len(t)):
							latest_raw_data.iloc[t[j],0] = date
					i = k
				else:
					i += 1
				if i == len(latest_raw_data) - 1:
					latest_raw_data.iloc[i,0] = latest_raw_data.iloc[i-1,0]
			print('  have transfromed trading-date to action-date. ')
		elif self._dataFrame == 1:
			print('  data frame: 1 mins ')
			i = 0
			while i < len(latest_raw_data):
				if latest_raw_data.iloc[i,1] == 1500:
					k = i + 1
					t = []
					date = latest_raw_data.iloc[i,0]
					while k < len(latest_raw_data) and latest_raw_data.iloc[k,1] != 901:
						if latest_raw_data.iloc[k,1] >= 2101 and latest_raw_data.iloc[k,1] <= 2359:
							t.append(k)
						k += 1
					if len(t) != 0:
						for j in range(len(t)):
							latest_raw_data.iloc[t[j],0] = date
					i = k
				else:
					i += 1
				if i == len(latest_raw_data) - 1:
					latest_raw_data.iloc[i,0] = latest_raw_data.iloc[i-1,0]
			print('  have transfromed trading-date to action-date. ')
		latest_transformed_data = latest_raw_data
		return latest_transformed_data

	def cut(self,latest_transformed_data):
		print('  date length:' + str(len(latest_transformed_data)))
		t5 = []; t = 1; k = 1; u = 1
		while t == k:
			for i in range(len(latest_transformed_data)):
				if (i+1) % 5 == 0:
					t5.append(latest_transformed_data.iloc[i,1])
					if t5[-1] % 5 != 0:
						idx = i - np.where(latest_transformed_data.iloc[range(i,i-3,-1),1] - \
										latest_transformed_data.iloc[range(i-1,i-4,-1),1] != 1)
						latest_transformed_data.drop([idx],inplace=True) #inplace=True直接改变内存的值
						latest_transformed_data = latest_transformed_data.reset_index(drop=True) #删除原来索引，重新建立从0开始的索引
						print('  have cleared redundant data ' + str(u) + ', and data length is ' + \
							str(len(latest_transformed_data)) + ' right now. ')
						u += 1
						break
			if i == len(latest_transformed_data) - 1:
				k += 1
				print('  redundant data not exist anymore. ')
		processed_data = latest_transformed_data
		return processed_data

	def data_processing(self,symbol_path):
		latest_raw_data = self.gen_data_from_txt(symbol_path)
		# 1.transfrom trading-day to action-day
		latest_transformed_data = self.transfrom(latest_raw_data)
		# 2.clear redundant data
		if self._dataFrame == 5:
			processed_data = self.cut(latest_transformed_data)
		else:
			processed_data = latest_transformed_data
		return processed_data

	def extract_info(self,tag_list,collectionName):
		collection = self.conn_mongodb(collectionName)
		tag_data = []
		for tag in tag_list:
			exec(tag + " = collection.distinct('" + tag + "')")
			exec("tag_data.append(" + tag + ")")
		return tag_data

	def insert_to_database(self,symbol_path,symbol):
		collection = self.conn_mongodb(symbol)
		latest_processed_data = self.data_processing(symbol_path)
		# 1.extract specific info from db
		date_distinct_list = self.extract_info(['Date'],symbol)
		if date_distinct_list[0] == []:
			for i in range(len(latest_processed_data)):
				data = {'_id' : str(i),
						'Date' : str(latest_processed_data.iloc[i,0]),
						'Time' : str(latest_processed_data.iloc[i,1]),
						'Open' : str(latest_processed_data.iloc[i,2]),
						'High' : str(latest_processed_data.iloc[i,3]),
						'Low' : str(latest_processed_data.iloc[i,4]),
						'Close' : str(latest_processed_data.iloc[i,5]),
						'Volumn' : str(latest_processed_data.iloc[i,6]),
						'OpenInterest' : str(latest_processed_data.iloc[i,7])}
				collection.insert_one(data)
			print(' * finish inserting ' + symbol + ' data. ')
		else:
			# 2.duplicate removal and insert latest data to database
			date = np.int64(date_distinct_list[0])
			last_date = str(np.max(date))
			data = collection.find({'Date':last_date}) #return Object
			last_time = max([int(k["Time"]) for k in data])
			latest_processed_data_1 = latest_processed_data[latest_processed_data.Date==int(last_date)]
			start_insert_ind =  latest_processed_data_1[latest_processed_data_1.Time==last_time].index[0]
			for i in range(len(latest_processed_data)):
				if i > start_insert_ind:
					data = {'_id' : str(i),
							'Date' : str(latest_processed_data.iloc[i,0]),
							'Time' : str(latest_processed_data.iloc[i,1]),
							'Open' : str(latest_processed_data.iloc[i,2]),
							'High' : str(latest_processed_data.iloc[i,3]),
							'Low' : str(latest_processed_data.iloc[i,4]),
							'Close' : str(latest_processed_data.iloc[i,5]),
							'Volumn' : str(latest_processed_data.iloc[i,6]),
							'OpenInterest' : str(latest_processed_data.iloc[i,7])}
					collection.insert_one(data)
			print(' * finish removing duplication and inserting latest ' + symbol + ' data. ')
		
	def multi_thread_run(self,max_threads_num,file_path):
		symbol_info_list = self.output_symbol_list(file_path)
		#symbol_info[0] is symbol path; symbol_info[1] is symbol name
		with futures.ThreadPoolExecutor(max_workers=max_threads_num) as executor:
			future_to_symbol = {executor.submit(self.insert_to_database,symbol_info[0],symbol_info[1]) : \
			ind for ind, symbol_info in enumerate(symbol_info_list)} 

if __name__ == '__main__':
	Data_Info_Dist = {"dataframe":[1,5],"databasename":["futures_1min_data","futures_5min_data"],\
	"filepath":["D:\\Quant_Python\\tongdaxin_data\\1min_txt","D:\\Quant_Python\\tongdaxin_data\\5min_txt"]}
	for i in range(len(Data_Info_Dist["dataframe"])):
		tmo = tdx_mongodb_operation("localhost",27017,dataframe=Data_Info_Dist["dataframe"][i],\
			database=Data_Info_Dist["databasename"][i])
		tmo.multi_thread_run(max_threads_num=4,file_path=Data_Info_Dist["filepath"][i])
