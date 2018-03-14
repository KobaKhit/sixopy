import pyodbc
import time
from tqdm import *
import sixopy.utils as u


class Odbc(object):
	"""docstring for ClassName"""
	def __init__(self,server,database,username,password, driver = '{ODBC Driver 13 for SQL Server}'):
		super().__init__() # http://stackoverflow.com/questions/576169/understanding-python-super-with-init-methods
		self.server = server
		self.database = database
		self.username = username
		self.password = password
		self.driver = driver
		self.cursor = self.connect_to_azure()

	def connect_to_azure(self):
	    # connect to database
	    cnxn = pyodbc.connect('DRIVER='+self.driver+';PORT=1433;SERVER='+self.server+';PORT=1443;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
	    return(cnxn.cursor())

	def close(self):
		self.cursor.close()


	def create_table(self,tablename, fieldnames, fieldtypes, deleteExisting = False,query=None):
		# Creates table in the database.
		# Args:
		# - tablename - string. Name of the table.
		# - fields - list. List of field names.
		# - fieldtypes - list. List of corresponding fieldtypes.
		# - deleteExisting - bool. If set to True will delete existing table and create new one.

		# connect to database
		cursor = self.cursor
		# check if table exists
		exists = cursor.tables(table=tablename.split('.')[-1], tableType='TABLE').fetchone() 
		
		# use user supplied or generated query
		if query is None:
			# add brackets for field names with spaces (SQL syntax)
			fieldnames = ['[' + str(x) + ']' if ' ' in x and '[' not in x else x for x in fieldnames]
			fandt = ','.join(["{} {}".format(a,b) for a,b in zip(fieldnames, fieldtypes)])
			create_query = "CREATE TABLE " + tablename + "(" + fandt + ")"
		else:
			create_query = query # user input

		if exists and deleteExisting:
			try:
				# delete table
				cursor.execute("DROP TABLE " + tablename)
				cursor.commit()
				print("Dropped " + tablename)
				#create table
				cursor.execute(create_query)
				cursor.commit()
				print("Created " + tablename)
			except Exception as e:
				print(e)
				pass
		else:
			try:
				#create table
				cursor.execute(create_query)
				cursor.commit()
				print("Created " + tablename)
			except Exception as e:
				print(e)
				if '42S01' in e.args:
					print("You may set deleteExisting flag to True to delete and recreate the table.")
				pass

		# cursor.close()

	def select(self, query):
		# Executes a given SELECT query.
		# Args:
		# - query - string. A valid SQL SELECT query.

		cursor = self.cursor
		cursor.execute(query)
		return(cursor.fetchall())

	def insert(self, tablename, columnnames, values):
		# Insert rows into table.
		# Args:
		# - tablename - string. 
		# - columnnames - list. List of columnnames.
		# - values - list. List of values.

		cursor = self.cursor
		marks = ','.join(['?' for i in range(0,len(columnnames))])
		columnnames = ','.join(['[' + str(x) + ']' if ' ' in x and '[' not in x else x for x in columnnames])
		query = 'INSERT INTO ' + tablename + ' (' + columnnames + ') VALUES' + ' (' + marks + ')'
		cursor.execute(query, values)
		cursor.commit()
		# cursor.close()

	def insert_list(self, tablename, columnnames, valueslist):
		# Insert rows into table from a list of lists.
		# Args:
		# - tablename - string. 
		# - columnnames - list. List of columnnames.
		# - valuesList - list. List of lists of values, ex.g. [[val11,val12], [val21,val22], ...]
		for r in tqdm(valueslist):
			self.insert(tablename,columnnames,r)

	def update(self, tablename, columnnames, idcol, values,idd):
		# Update rows in table based on id.
		# Args:
		# - tablename - string. 
		# - columnnames - list. List of columnnames.
		# - idcol - string. Name of id column by which to update.
		# - values - list. List of values corresponding to each column.
		# - idd - id value.

		cursor = self.cursor
		query = 'UPDATE ' + tablename + ' SET ' +'=?,'.join(columnnames)+'=? WHERE ' + idcol + '=?'

		values.extend(idd)
		cursor.execute(query, values)
		cursor.commit()
		# cursor.close()



	def update_list(self, tablename, columnnames, idcol, valueslist, iddsList):
		# Update rows in table from a list of lists.
		# Args:
		# - tablename - string. 
		# - columnnames - list. List of columnnames.
		# - idcol - string. Name of id column by which to update.
		# - valuesList - list. List of lists of values, ex.g. [[val11,val12], [val21,val22], ...]
		# - iddsList - list. List of ids by which to update.
		for i in tqdm(range(0,len(valueslist))):
			self.update(tablename,columnnames,idcol,valueslist[i],iddsList[i])

		# cursor.close()



def main():
	# sql database creds
	server = 'your_server'
	database = 'your_db'
	username = 'your_username'
	password = 'your_pass'

	# my creds
	f = u.parse_config(r'C:\Users\kkhitalishvili\Desktop\creds.txt')
	server, db, uname, pas = dict(f.items('azure')).values()
	azure = Odbc(server,db,uname,pas)
	
	# print(azure.query("SELECT [id str] from sixers.tweets_sixers"))

	#azure.create_table('sixers.test',['id','big'],['varchar(55)','varchar(55)'],deleteExisting = True)
	# azure.insert_list2('sixers.test', ['id','big'], [[x,'a'] for x in range(0,100)])
	ids = azure.select('SELECT * from sixers.tweets_sixers')
	ids = [m[0] for m in ids]
	print(ids[0])

	# azure.update('sixers.test',['big'],'id',['b'],'0')

	# azure.update_list('sixers.test',['big'],'id',\
	# 	[['b'] for x in range(5,50)],\
	# 	[[x] for x in range(5,50)])

	# df = azure.select("SELECT * from sixers.facebook_posts")
	# l = [(m[0],m[1]) for m in df]
	# azure.insert_list('sixers.test',['big','small'], l, verbose=True)


if __name__ == '__main__':
	main()