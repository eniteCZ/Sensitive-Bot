import sqlite3
import TextBlob

conn = sqlite.connect(r"")
c = conn.cursor()
parent_SA = []
comment_SA = []

while cur_len == limit:
	df = pd.read_sql("SELECT parent_id,parent,comment FROM interactions WHERE parent_id > '{}' ORDER BY parent_id ASC LIMIT {}".format(last_id, limit), conn)
	last_id = df.tail(1)['parent_id'].values[0]
	cur_len = len(df)
	for comment in df['parent','comment']:
		parent_SA = parent_SA.append(analyzesentiment(comment[0]))
		comment_SA = comment_SA.append(analyzesentiment(comment[1]))
	counter +=1
	if counter % 20 == 0:
		print(counter*limit, ' rows completed')




