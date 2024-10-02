import mysql.connector
import time,configparser
from mysql.connector import connect, Error,errors
connection_2 = None
#In this code you'll see some shit...
class mysql_c:  
	db_name = ""
	dbhost = ""
	dbuser = ""
	dbpass = ""
	def __init__(self, dbhost, dbuser, dbpass, db_name):
		global connection_2
		self.db_name = db_name
		self.dbhost = dbhost
		self.dbuser = dbuser
		self.dbpass = dbpass
		cfg = configparser.ConfigParser()
		cfg.read("tracker.cfg")
		self.interval = cfg.getint("OPTIONS","announce_refresh")
		try:
			connection_2 = mysql.connector.connect(
				host=dbhost,
				user=dbuser,
				password=dbpass,
				database=db_name,
				autocommit=True,
				connection_timeout=86400
			)
			self.checktables()
		except Error as e:
			print(e)
			
	def checktables(self):
		global connection_2
		dbs = []
		with connection_2.cursor() as cursor:
			cursor.execute("SHOW TABLES FROM "+self.db_name)
			result = cursor.fetchall()
			if(not len(result) == 0):
				for row in result:
					dbs.append(row[0])
				if("tracker_torrents" not in dbs):
					print("Table tracker_torrents not found. Creating...")
					cursor.execute("create table if not exists tracker_torrents (id int not null auto_increment,info_hash varchar(64) not null,seeders int not null default 0,leechers int not null default 0,tsize bigint unsigned not null default 0,completed int not null default 0, updated int not null default 0,primary key (id),unique key (info_hash),index upd (updated));")
				if("tracker_users" not in dbs):
					print("Table tracker_users not found. Creating...")
					cursor.execute("create table if not exists tracker_users (id int not null auto_increment,peerhash varchar(64) not null,peerid varchar(40) not null,addr varchar(128) not null,port int not null default 0,ctime int not null default 0,utime int not null default 0,useragent varchar(256) not null,primary key (id),unique key (peerhash));")

				if("tracker_tpeers" not in dbs):
					print("Table tracker_tpeers not found. Creating...")
					cursor.execute("create table if not exists tracker_tpeers (id int not null auto_increment,pid varchar(64) not null,tid varchar(64) not null,uploaded bigint unsigned not null default 0,completed int not null default 0,downloaded bigint unsigned not null default 0,mtime int not null,primary key (id),index pid_key (pid),index tid_key (tid));")

				if("tracker_stats" not in dbs):
					print("Table tracker_stats not found. Creating...")
					cursor.execute("create table if not exists tracker_stats (id int not null auto_increment,reqs double(6,2) not null default 0,seeds int(11) not null default 0, leech int(11) not null default 0, users int(11) not null default 0, torrents int(11) not null default 0, tstamp int(11) not null default 0,primary key (id),unique key (tstamp));")

				print("DB initialized. Starting tracker server")
			else:
				print("DB connection failed. Tables not found.",len(result))
	
	def log(self,req_stats2,torrents,users):
		global connection_2,req_stats
		self.reconnect()
		print("Update stats")
		if(self.timestamp()-req_stats2['last_log'] == 0):
			self.query_update("INSERT INTO tracker_stats (id,reqs,seeds,leech,users,torrents,tstamp) VALUES (0,'"+str(round(req_stats2['ann']/(self.timestamp() - req_stats2['start_time']),2))+"','"+str(req_stats2['users']['seaders'])+"','"+str(req_stats2['users']['leechers'])+"','"+str(len(users))+"','"+str(len(torrents))+"','"+str(self.timestamp())+"')")
		else:
			self.query_update("INSERT INTO tracker_stats (id,reqs,seeds,leech,users,torrents,tstamp) VALUES (0,'"+str(round((req_stats2['ann']-req_stats2['last_ann'])/(self.timestamp()-req_stats2['last_log']),2))+"','"+str(req_stats2['users']['seaders'])+"','"+str(req_stats2['users']['leechers'])+"','"+str(len(users))+"','"+str(len(torrents))+"','"+str(self.timestamp())+"')")
		print("Update users")
		data = []
		datastr = ""
		tru = self.query("SELECT * from tracker_users")
		u = {}
		if(tru):
			for rw in tru:
				u[rw['peerhash']] = rw['id']
		print("fetched",len(u),"users")
		for user in list(users):
			fnd = False
			id = 0
			if(tru):
				if(user in u):
					id = u[user]
					fnd = True
			if(not fnd):
				if('UA' not in users[user] or users[user]['UA'] is None):
					users[user]['UA'] = "Not Defined"
				data.append(tuple((0,user, str(users[user]['peerid'].encode('utf8','surrogateescape').hex()).encode(), users[user]['addr'], users[user]['port'], users[user]['created'], users[user]['updated'], users[user]['UA'])))
			else:
				datastr += "UPDATE tracker_users SET utime='"+str(users[user]['updated'])+"' WHERE id='"+str(id)+"';"
		tru = None
		u = {}
		if(len(data) > 0):
			print("Inserting",len(data),"users")
			qq = "INSERT INTO tracker_users (id,peerhash,peerid,addr,port,ctime,utime,useragent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
			self.query_multiinsert(qq,data)
		if(datastr != ""):
			self.ihate_query_update(datastr)
		data = []
		datastr = ""
		print("Update torrents")
		trr = self.query("SELECT * from tracker_torrents")
		trrows = {}
		if(trr):
			for rw in trr:
				trrows[rw['info_hash']] = rw
		trr = None
		print("fetched",len(trrows),"torrents")
		trr = self.query("SELECT * from tracker_tpeers")
		tpeers = {}
		if(trr):
			for rw in trr:
				if(rw['tid'] not in tpeers):
					tpeers[rw['tid']] = {}
				tpeers[rw['tid']][rw['pid']] = rw['id']
		trr = None
		data2 = []
		data2str = ""
		for torrent in list(torrents):
			fnd = False
			trow = {}
			if(bool(trrows)):
				if(torrent in trrows):
					trow = trrows[torrent]
					fnd = True
			if(not fnd):
				data.append(tuple((0,torrent, torrents[torrent]['seaders'],torrents[torrent]['leechers'],torrents[torrent]['size'],torrents[torrent]['completed'],torrents[torrent]['updated'])))
			else:
				if(trow['completed'] > torrents[torrent]['completed']):
					torrents[torrent]['completed'] = torrents[torrent]['completed'] + trow['completed']
				if(trow['tsize'] < torrents[torrent]['size'] and torrents[torrent]['size'] < 1099511627776):
					datastr += "UPDATE tracker_torrents SET seeders='"+str(torrents[torrent]['seaders'])+"',leechers='"+str(torrents[torrent]['leechers'])+"',tsize='"+str(torrents[torrent]['size'])+"',completed='"+str(torrents[torrent]['completed'])+"' WHERE info_hash='"+torrent+"';"
				else:
					datastr += "UPDATE tracker_torrents SET seeders='"+str(torrents[torrent]['seaders'])+"',leechers='"+str(torrents[torrent]['leechers'])+"',completed='"+str(torrents[torrent]['completed'])+"',updated='"+str(torrents[torrent]['updated'])+"' WHERE info_hash='"+torrent+"';"
			
			for user in list(torrents[torrent]['users']):
				if(torrent in torrents and user in torrents[torrent]['users']):
					if(torrent not in tpeers or user not in tpeers[torrent]):
						data2.append(tuple((0,user,torrent,torrents[torrent]['users'][user]['uploaded'],torrents[torrent]['users'][user]['complete'],torrents[torrent]['users'][user]['downloaded'],torrents[torrent]['users'][user]['timestamp'])))
					else:
						data2str += "UPDATE tracker_tpeers SET uploaded='"+str(torrents[torrent]['users'][user]['uploaded'])+"',completed='"+str(int(torrents[torrent]['users'][user]['complete']))+"',downloaded='"+str(torrents[torrent]['users'][user]['downloaded'])+"',mtime='"+str(torrents[torrent]['users'][user]['timestamp'])+"' WHERE id='"+str(tpeers[torrent][user])+"';"
		if(len(data2) > 0):
			qq = "INSERT INTO tracker_tpeers (id,pid,tid,uploaded,completed,downloaded,mtime) VALUES (%s,%s,%s,%s,%s,%s,%s)"
			self.query_multiinsert(qq,data2)
		if(data2str != ""):
			self.ihate_query_update(data2str)
		data2 = []
		data2str = ""
		trrows = None
		if(len(data) > 0):
			qq = "INSERT INTO tracker_torrents (id,info_hash,seeders,leechers,tsize,completed,updated) VALUES (%s,%s,%s,%s,%s,%s,%s)"
			self.query_multiinsert(qq,data)
		if(datastr != ""):
			self.ihate_query_update(datastr)
		self.query_update("DELETE FROM tracker_users WHERE utime < "+str(self.timestamp() - int(self.interval*1.2)))
		self.query_update("DELETE FROM tracker_torrents WHERE updated < "+str(self.timestamp() - int(self.interval*1.2)))
		self.query_update("DELETE FROM tracker_tpeers WHERE mtime < "+str(self.timestamp() - int(self.interval*1.2)))
		print("Update complete")
		
	def timestamp(self):
		t = time.time()
		t = int(round(t,0))
		return t
	
	def reconnect(self):
		global connection_2
		try:
			connection_2 = mysql.connector.connect(
				host=self.dbhost,
				user=self.dbuser,
				password=self.dbpass,
				database=self.db_name,
				autocommit=True,
				connection_timeout=86400
			)
			print("Mysql reconnect. Reconnect.", connection_2.is_connected())
		except Error as e:
			print(e)

	def loadtorrents(self):
		self.reconnect()
		q = self.query("SELECT * from tracker_torrents")
		torrents = {}
		if(q):
			for row in q:
				if(row['leechers'] > 0 or row['seeders'] > 0):
					torrents[row['info_hash']] = {"users":{},"leechers":0,"seaders":0,"size":row['tsize'],"completed":row['completed'],"updated":row['updated']}
			print("loaded",len(torrents),"torrents From DB")
		return torrents
	def query_multiinsert(self,qr,data,tryes=0):
		global connection_2
		#dbs = {}
		if(tryes < 5):
			try:
				#print("req:",qr)
				if(connection_2.is_connected()):
					with connection_2.cursor() as cursor:
						cursor.executemany(qr,data)
						connection_2.commit()
						if(cursor.rowcount):
							cursor.close()
							return True
						else:
							cursor.close()
							return False
				else:
					print("Mysql connection failed. Reconnect.", connection_2.is_connected())
					connection_2.close()
					self.reconnect()
					return self.query_multiinsert(qr,data,tryes+1)
			except mysql.connector.Error as err:
				print(qr,"Something went wrong: {}".format(err))
				#if(err.errno == 1213):
					#self.query_multiinsert(qr,data,tryes+1)
		else:
			print("MYSQL ERROR. Max reconnect reached")	
	
	def query_update(self,qr,tryes=0):		
		global connection_2
		#dbs = {}
		if(tryes < 5):
			try:
				#print("req:",qr)
				if(connection_2.is_connected()):
					with connection_2.cursor() as cursor:
						cursor.execute(qr)
						connection_2.commit()
						if(cursor.rowcount):
							return True
						else:
							return False
				else:
					print("Mysql connection failed. Reconnect.", connection_2.is_connected())
					connection_2.close()
					self.reconnect()
					return self.query_update(qr,tryes+1)
			except mysql.connector.Error as err:
				print(qr,"Something went wrong: {}".format(err))
		else:
			print("MYSQL ERROR. Max reconnect reached")
			
	def ihate_query_update(self,qr,tryes=0):
		global connection_2
		#dbs = {}
		if(tryes < 5):
			try:
				#print("req:",qr)
				if(connection_2.is_connected()):
					with connection_2.cursor() as cursor:
						query_string = qr.split(';')
						str = ""
						t = 0
						if(len(query_string) > 500):
							for qstr in query_string:
								str += qstr +";"
								t += 1
								if(t >= 500):
									t=0
									for result in cursor.execute(str,multi=True):
										pass
									connection_2.commit()
									str = ""
						if(len(str) != 0):
							for result in cursor.execute(str,multi=True):
								pass
						connection_2.commit()
						if(cursor.rowcount):
							cursor.close()
							return True
						else:
							cursor.close()
							return False
				else:
					print("Mysql connection failed. Reconnect.", connection_2.is_connected())
					connection_2.close()
					self.reconnect()
					return self.ihate_query_update(qr,tryes+1)
			except mysql.connector.Error as err:
				print(qr,"Something went wrong: {}".format(err))
		else:
			print("MYSQL ERROR. Max reconnect reached")

	def query(self,qr,tryes=0):
		global connection_2
		#dbs = {}
		if(tryes < 5):
			try:
				#print("req:",qr)
				if(connection_2.is_connected()):
					with connection_2.cursor(buffered=True,dictionary=True) as cursor:
						cursor.execute(qr)
						if(cursor.rowcount):
							result = cursor.fetchall()
							cursor.close()
							#for row in result:
							#	dbs.append(row)
							return result
						else:
							return False
				else:
					print("Mysql connection failed. Reconnect.", connection_2.is_connected())
					connection_2.close()
					self.reconnect()
					return self.query(qr,tryes+1)
			except Error as e:
				print("Mysql error ",e)
		else:
			print("MYSQL ERROR. Max reconnect reached")
