import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\Users\fradm\Desktop\School_stuff\Thesis\Code\TraningSet.db")
c = conn.cursor()
#Settings
limit = 5000
last_id = 0
cur_len = limit
counter = 0
test_created = False
while cur_len == limit:
	df = pd.read_sql("SELECT parent_id,parent,comment FROM interactions WHERE parent_id > {} ORDER BY parent_id ASC LIMIT {}".format(last_id, limit), conn)
	last_id = df.tail(1)['parent_id'].values[0]
	cur_len = len(df)
	if not test_created:
		#This creates a test file containing first 5000 comments-responses and after that is ignored
		with open("C:\Users\fradm\Desktop\School_stuff\Thesis\Code\test.from",'a',encoding= 'utf8') as f:
			for com in df['parent'].values:
				f.write(content+'\n')
		with open("C:\Users\fradm\Desktop\School_stuff\Thesis\Code\test.to",'a',encoding= 'utf8') as f:
			for com in df['comment'].values:
				f.write(content+'\n')
		test_created = True
	else:
		with open("C:\Users\fradm\Desktop\School_stuff\Thesis\Code\training.from",'a',encoding= 'utf8') as f:
			for com in df['parent'].values:
				f.write(content+'\n')
		with open("C:\Users\fradm\Desktop\School_stuff\Thesis\Code\training.to",'a',encoding= 'utf8') as f:
			for com in df['comment'].values:
				f.write(content+'\n')
	counter +=1
	if counter % 20 == 0:
		print(counter*limit, ' rows completed')