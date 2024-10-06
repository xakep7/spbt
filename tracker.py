import time
import argparse
from datetime import datetime
import random
import math
import os
import ipaddress
import cgitb
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from http.server import HTTPServer
import bencodepy
import hashlib
import urllib,gc
import threading,struct,netaddr,configparser,socket
from mysql_log import *
from signal import signal, SIGPIPE, SIG_DFL
from socket import error as SocketError
import errno

vers = "SPBT v0.4.9"
server_host = ''
server_port = 8050
interval = 1800
minint = 60
stime = 0
hunghttpd = None
scrape_int = 120
cf_header = "X-Forwarded-For"
torrents = {}
users = {}
req_stats = {"ann":0,"scrape":0,"users":{"seaders":0,"leechers":0},"last_log":0,"last_ann":0,"start_time":0}
cgitb.enable()
signal(SIGPIPE,SIG_DFL)
last_clean = 0

def is_json(json_arr):
	try:
		json.loads(json_arr)
	except ValueError as e:
		return False
	return True

def timestamp():
	t = time.time()
	t = int(round(t,0))
	return t

def run(server_class=HTTPServer, handler_class=BaseHTTPRequestHandler):
	global hunghttpd
	handler_class=HttpGetHandler
	httpd = ThreadingHTTPServer( (server_host, server_port), handler_class)
	hunghttpd = httpd
	#httpd.serve_forever()
	#server_address = ('', server_port)
	#httpd = server_class(server_address, handler_class) #older singlethread server.
	try:
		print(time_s()," Tracker started")
		httpd.serve_forever()
		print(time_s()," Tracker working")
	except KeyboardInterrupt:
		httpd.server_close()
def remove_array_item(array, item):
    return [x for x in array if x != item]
	
class HttpGetHandler(BaseHTTPRequestHandler):
	def log_message(*args): pass
	def do_GET(self):
		self.server_version = vers
		self.sys_version = ""
		try:
			if(self.path != ""):
				path2 = self.path.split('?')
				get_req = {}
				if(len(path2) != 1 and len(path2) < 3):
					query = urllib.parse.unquote(path2[1], errors="surrogateescape")
					get_req = urllib.parse.parse_qs(query)
					for var in get_req:
						if(get_req[var][0].isnumeric()):
							get_req[var] = int(get_req[var][0])
						else:
							get_req[var] = get_req[var][0]
				if(path2[0] == "/announce"):
					self.send_response(200)
					#self.send_header("Server", vers)
					self.end_headers()
					if('info_hash' in get_req):
						req_stats['ann'] += 1
						self.announce(get_req)
					else:
						self.wfile.write(bencodepy.bencode({"failure reason":"Incorrect request","min interval":minint}))
				elif(path2[0] == "/scrape"):
					self.send_response(200)
					#self.send_header("Server", vers)
					self.end_headers()
					if('info_hash' in get_req):
						req_stats['scrape'] += 1
						self.scrape(get_req)
					else:
						self.wfile.write(bencodepy.bencode({"failure reason":"Incorrect request","min interval":minint}))
				elif(path2[0] == "/stats"):	
					self.send_response(200)
					self.send_header("Content-type", "application/json; charset=utf-8")
					self.end_headers()
					if(req_stats['last_log'] == 0):
						txt = json.dumps({"torrents": len(torrents), "seeders": req_stats['users']['seaders'],"users":len(users), "leechers": req_stats['users']['leechers'], "annonces":req_stats['ann'], "scrapes":req_stats['scrape'], "ann_req":round(req_stats['ann']/(time.time()-stime),2)}, sort_keys=True)
					else:
						txt = json.dumps({"torrents": len(torrents), "seeders": req_stats['users']['seaders'],"users":len(users), "leechers": req_stats['users']['leechers'], "annonces":req_stats['ann'], "scrapes":req_stats['scrape'], "ann_req":round((req_stats['ann']-req_stats['last_ann'])/(timestamp()-req_stats['last_log']),2)}, sort_keys=True)
					self.wfile.write(txt.encode())
				else:
					self.send_error(404,"<body>Requested path " + str(self.path) + " is not found.</body></html>")
					#self.send_header("Server", vers)
					self.end_headers()
					self.wfile.write('<html><head><meta charset="utf-8">'.encode())
					self.wfile.write('<title>404 Not found.</title></head>'.encode())
					txt = "<body>Requested path " + str(self.path) + " is not found.</body></html>"
			else:
				self.send_error(404,"Requested path " + str(self.path) + " is not found.")
				#self.send_header("Server", vers)
				self.end_headers()
				self.wfile.write('<html><head><meta charset="utf-8">'.encode())
				self.wfile.write('<title>404 Not found.</title></head>'.encode())
				txt = "<body>Requested path " + str(self.path) + " is not found.</body></html>"
				self.wfile.write(txt.encode())
		except BrokenPipeError as e:
			pass
		except SocketError as e:
			if e.errno != errno.ECONNRESET:
				raise # Not error we are looking for
			pass # Handle error here.
	
	def scrape(self,get_req):
		if(isinstance(get_req['info_hash'], int)):
			get_req['info_hash'] = str(get_req['info_hash']).encode('utf-8', 'strict')
		else:
			get_req['info_hash'] = get_req['info_hash'].encode('utf8','surrogateescape')
		if(len(get_req['info_hash']) != 20):
			get_req['info_hash'] = get_req['info_hash'].hex()
			self.wfile.write(bencodepy.bencode({"failure reason":"Invalid info_hash: '"+ get_req['info_hash'] +"' length "+ str(len(get_req['info_hash'])),"min interval":minint}))
			return 0
		get_req['info_hash'] = get_req['info_hash'].hex()
		if(get_req['info_hash'] in torrents):
			seeds = leech = 0
			for user in list(torrents[get_req['info_hash']]['users']):
				#print("peerhash:" , user)
				if(torrents[get_req['info_hash']]['users'][user]['timestamp'] < timestamp() - interval*1.2):
					#print(self.log_date_time_string()," delete user ", torrents[get_req['info_hash']]['users'][user]['peer']," timer reached: ", (time.time() - interval), ">",torrents[get_req['info_hash']]['users'][user]['timestamp'])
					if(torrents[get_req['info_hash']]['users'][user]['complete']):
						torrents[get_req['info_hash']]['seaders'] -= 1
						req_stats['users']['seaders'] -= 1
					else:
						torrents[get_req['info_hash']]['leechers'] -= 1
						req_stats['users']['leechers'] -= 1
					if(torrents[get_req['info_hash']]['users'][user]['peer'] in users):
						users[torrents[get_req['info_hash']]['users'][user]['peer']]['torrs'] = remove_array_item(users[torrents[get_req['info_hash']]['users'][user]['peer']]['torrs'],get_req['info_hash'])
					del torrents[get_req['info_hash']]['users'][user]
				else:
					if(torrents[get_req['info_hash']]['users'][user]['complete']):
						seeds = seeds + 1
					else:
						leech = leech + 1
			self.wfile.write(bencodepy.bencode({"files":{get_req['info_hash']:{"complete":seeds,"incomplete":leech,"downloaded":seeds}},"flags":{"min_request_interval":scrape_int}}))
		else:
			self.wfile.write(bencodepy.bencode({"failure reason":"Invalid torrent","min interval":minint}))
		
	def announce(self,get_req):
		if('info_hash' in get_req):
			#get_req['info_hash'] = str(binascii.hexlify(str.encode(get_req['info_hash'])))
			if(isinstance(get_req['info_hash'], int)):
				get_req['info_hash'] = str(get_req['info_hash']).encode('utf-8', 'strict')
			else:
				get_req['info_hash'] = get_req['info_hash'].encode('utf8','surrogateescape')
			if(len(get_req['info_hash']) != 20):
				get_req['info_hash'] = get_req['info_hash'].hex()
				self.wfile.write(bencodepy.bencode({"failure reason":"Invalid info_hash: '"+ get_req['info_hash'] +"' length "+ str(len(get_req['info_hash'])),"min interval":minint}))
				return 0
			get_req['info_hash'] = get_req['info_hash'].hex()
			if('port' in get_req):
				if(not isinstance(get_req['port'],int)):
					get_req['port'] = int(get_req['port'])
				if(get_req['port'] < 0 or get_req['port'] > 65535):
					self.wfile.write(bencodepy.bencode({"failure reason":"Invalid port","min interval":minint}))
					return 0
			else:
				self.wfile.write(bencodepy.bencode({"failure reason":"Port not set","min interval":minint}))
				return 0
			if('peer_id' in get_req):
				if(isinstance(get_req['peer_id'], int)):
					get_req['peer_id'] = str(get_req['peer_id']).encode('utf-8', 'strict')
				if(len(get_req['peer_id']) != 20):
					self.wfile.write(bencodepy.bencode({"failure reason":"Invalid peer_id","min interval":minint}))
					return 0
			else:
				self.wfile.write(bencodepy.bencode({"failure reason":"Invalid peer_id","min interval":minint}))
				return 0
			if('uploaded' in get_req):
				if(get_req['uploaded'] < 0):
					self.wfile.write(bencodepy.bencode({"failure reason":"Invalid uploaded","min interval":minint}))
					return 0
			if('downloaded' in get_req):
				if(get_req['downloaded'] < 0):
					self.wfile.write(bencodepy.bencode({"failure reason":"Invalid downloaded","min interval":minint}))
					return 0
			if('left' in get_req):
				if(not isinstance(get_req['left'],int)):
					try:
						get_req['left'] = int(get_req['left'])
					except:
						get_req['left'] = 0
				if(get_req['left'] < 0):
					self.wfile.write(bencodepy.bencode({"failure reason":"Invalid left","min interval":minint}))
					return 0
			if('left' not in get_req):
				get_req['left'] = 0
			if(cf_header in self.headers):
				ip = self.headers[cf_header].split(",")[0]
			else:
				ip = self.client_address[0]
			peerhash = str(hashlib.md5(str(get_req['peer_id'].encode('utf8','surrogateescape').hex()).encode() + ip.encode() + str(get_req['port']).encode()).hexdigest())
			if(get_req['info_hash'] not in torrents):
				torrents[get_req['info_hash']] = {"users":{},"leechers":0,"seaders":0,"size":0,"completed":0,"updated":timestamp()}
			else:
				torrents[get_req['info_hash']]['updated'] = timestamp()
			if(get_req['left'] == 0):
				complete = True
			else:
				complete = False
			if('size' in get_req):
				if(get_req['size'] > 0):
					size = get_req['size']
			else:
				size = get_req['left']
			if(torrents[get_req['info_hash']]['size'] < size):
				torrents[get_req['info_hash']]['size'] = size
			if(peerhash not in torrents[get_req['info_hash']]['users']):
				if(complete):
					torrents[get_req['info_hash']]['seaders'] += 1
					req_stats['users']['seaders'] += 1
					torrents[get_req['info_hash']]['completed'] += 1
				else:
					torrents[get_req['info_hash']]['leechers'] += 1
					req_stats['users']['leechers'] += 1
			if(peerhash not in users):
				users[peerhash] = {"peerid":get_req['peer_id'],"addr":ip,"port":get_req['port'],"created":timestamp(),"updated":timestamp(),"UA":self.headers['User-Agent'],"torrs":[get_req['info_hash']]}
			else:
				users[peerhash]['updated'] = timestamp()
				users[peerhash]['torrs'].append(get_req['info_hash'])
			torrents[get_req['info_hash']]['users'][peerhash] = {"peerid":get_req['peer_id'],"peer":peerhash,"uploaded":get_req['uploaded'],"downloaded":get_req['downloaded'],"complete":complete,"timestamp":timestamp()}
			seeds = leech = 0
			u = 0
			d = 0
			if('compact' in get_req):
				if(get_req['compact'] == 1):
					compact = 1
				else:
					compact = 0
			else:
				compact = 0
			if(compact == 1):
				peers = bytes()
				peers_ipv6 = bytes()
			else:
				peers = {}
				peers_ipv6 = {}
			for user in list(torrents[get_req['info_hash']]['users']):
				#print("peerhash:" , user)
				if(torrents[get_req['info_hash']]['users'][user]['timestamp'] < timestamp() - interval*1.2):
					if(torrents[get_req['info_hash']]['users'][user]['complete']):
						torrents[get_req['info_hash']]['seaders'] -= 1
						req_stats['users']['seaders'] -= 1
					else:
						torrents[get_req['info_hash']]['leechers'] -= 1
						req_stats['users']['leechers'] -= 1
					if(torrents[get_req['info_hash']]['users'][user]['peer'] in users):
						users[torrents[get_req['info_hash']]['users'][user]['peer']]['torrs'] = remove_array_item(users[torrents[get_req['info_hash']]['users'][user]['peer']]['torrs'],get_req['info_hash'])
					del torrents[get_req['info_hash']]['users'][user]
				else:
					if(torrents[get_req['info_hash']]['users'][user]['complete']):
						seeds = seeds + 1
					else:
						leech = leech + 1
					if(ipaddress.ip_address(users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr']).version == 4):
						if(compact == 1):
							peers += ipaddress.ip_address(users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr']).packed + struct.pack(">H",int(users[torrents[get_req['info_hash']]['users'][user]['peer']]['port']))
						else:
							peers[u] = {"peer id":users[torrents[get_req['info_hash']]['users'][user]['peer']]['peerid'],"ip":users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr'],"port":users[torrents[get_req['info_hash']]['users'][user]['peer']]['port']}
						u += 1
					else:
						if(compact == 1):
							#packed_ip = socket.inet_pton(socket.AF_INET6, users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr']) + struct.pack(">H",int(users[torrents[get_req['info_hash']]['users'][user]['peer']]['port']))
							peers_ipv6 += socket.inet_pton(socket.AF_INET6, users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr']) + struct.pack(">H",int(users[torrents[get_req['info_hash']]['users'][user]['peer']]['port']))
						else:
							peers_ipv6[d] = {"peer id":users[torrents[get_req['info_hash']]['users'][user]['peer']]['peerid'],"ip":users[torrents[get_req['info_hash']]['users'][user]['peer']]['addr'],"port":users[torrents[get_req['info_hash']]['users'][user]['peer']]['port']}
						d += 1
			#if(compact == 1):
			#	peers = str(peers)
				#peers_ipv6 = str(peers_ipv6)
			self.wfile.write(bencodepy.bencode({"interval":interval,"min interval":minint,"peers":peers,"peers6":peers_ipv6,"complete":seeds,"incomplete":leech}))
		else:
			self.wfile.write('<html><head><meta charset="utf-8">'.encode())
			self.wfile.write('<title>Nothing here.</title></head>'.encode())
			self.wfile.write('<body>Undefined action. Please see man page</body></html>'.encode())

def time_s():
	return datetime.strftime(datetime.now(), "%H:%M:%S")

def cleanup_users():
	global last_clean,users,torrents,req_stats
	while 1:
		ts = time_s()
		ds = ts.split(':')
		if(last_clean <= timestamp() - cleanup_int):
			print(time_s(),"Cleanup Started. Users:",len(users),"torrents",len(torrents)," leech:",req_stats['users']['leechers'],"seeds:",req_stats['users']['seaders'])
			for user in list(users):
				if(users[user]['updated'] < int(timestamp() - interval*1.2)):
					if('torrs' in users):
						for tr in list(users['torrs']):
							if(user in torrents[tr]['users']):
								if(torrents[tr]['users'][user]['complete']):
									torrents[tr]['seaders'] -= 1
									req_stats['users']['seaders'] -= 1
								else:
									torrents[tr]['leechers'] -= 1
									req_stats['users']['leechers'] -= 1
								del torrents[tr]['users'][user]
							if(torrents[tr]['seaders'] <= 0 and torrents[tr]['leechers'] <= 0):
								del torrents[tr]
					del users[user]
			for tr in list(torrents):
				if(torrents[tr]['seaders'] <= 0 and torrents[tr]['leechers'] <= 0):
					del torrents[tr]
				elif(torrents[tr]['updated'] < int(timestamp() - interval*1.2)):
					req_stats['users']['seaders'] -= torrents[tr]['seaders']
					req_stats['users']['leechers'] -= torrents[tr]['leechers']
					del torrents[tr]
			print(time_s(),"Cleanup complete. Users:",len(users),"torrents",len(torrents)," leech:",req_stats['users']['leechers'],"seeds:",req_stats['users']['seaders'])
			gc.collect()
			counts = gc.get_count()
			print(time_s(),"Cleanup garbage:",counts)
			last_clean = timestamp()
		time.sleep(1)

def logging():
	global mysql_c,mysql_reload,mysql_loging,req_stats,torrents,users,cfg
	reltime = int(round(mysql_reload / 60,0))
	msq_c = mysql_c(cfg.get("MYSQL","HOST"), cfg.get("MYSQL","USER"), cfg.get("MYSQL","PASSWORD"), cfg.get("MYSQL","NAME"))
	while 1:
		ts = time_s()
		ds = ts.split(':')
		if(req_stats['last_log'] <= timestamp() - mysql_reload and mysql_loging==1):
			#print("this time",reltime)
			if(int(float(ds[1])) % reltime == 0 and int(float(ds[2])) == 00):
				print(time_s()," Update stats: Started")
				if(req_stats['last_log'] == 0):
					print(time_s()," Update stats: need to sync time. Aborted. Last:",req_stats['last_log'])
					req_stats['last_log'] = timestamp()
					req_stats['last_ann'] = req_stats['ann']
					req_stats['start_time'] = stime
					mysql_logging_thread = threading.Thread(target=msq_c.log,args=(req_stats,torrents,users), daemon=True)
					mysql_logging_thread.start()
				else:
					mysql_logging_thread = threading.Thread(target=msq_c.log,args=(req_stats,torrents,users), daemon=True)
					mysql_logging_thread.start()
					req_stats['last_log'] = timestamp()
					req_stats['last_ann'] = req_stats['ann']
					print(time_s()," Update stats: Completed")
		time.sleep(1)
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--clear', action='store_true', help='clear the display on exit')
	args = parser.parse_args()
	print ('Press Ctrl-C to quit.')
	if not args.clear:
		print('Use "-c" argument to clear on exit')

	try:
		stime = timestamp()
		cfg = configparser.ConfigParser()
		cfg.read("tracker.cfg")
		server_port = cfg.getint("OPTIONS","serverport")
		server_host = cfg.get("OPTIONS","serverhost")
		interval = cfg.getint("OPTIONS","announce_refresh")
		minint = cfg.getint("OPTIONS","announce_min")
		scrape_int = cfg.getint("OPTIONS","scrape_int")
		cf_header = cfg.get("OPTIONS","cf_header")
		mysql_loging = cfg.getint("OPTIONS","mysql_store")
		mysql_reload = cfg.getint("OPTIONS","mysql_reload")
		cleanup_int = cfg.getint("OPTIONS","cleanup_interval")
		if(mysql_loging == 1):
			print (time_s(),' Mysql connection enabled')
			msq = mysql_c(cfg.get("MYSQL","HOST"), cfg.get("MYSQL","USER"), cfg.get("MYSQL","PASSWORD"), cfg.get("MYSQL","NAME"))
			torrents = msq.loadtorrents()
			del msq
		else:
			print (time_s(),' Mysql connection disabled ',mysql_loging)
		print (time_s(),' Starting server at port tcp ',server_host,':', server_port," header:",cf_header)
		httpdserverthread = threading.Thread(target=run, daemon=True)
		httpdserverthread.start()
		if(mysql_loging==1):
			print(time_s()," Start logging subprocces")
			logging = threading.Thread(target=logging,daemon=True)
			logging.start()
		cleanup_thread = threading.Thread(target=cleanup_users, daemon=True)
		cleanup_thread.start()
		gc.enable()
		gc.set_threshold(mysql_reload, int(mysql_reload/2), int(mysql_reload/4))
		httpdserverthread.join()
		#threading.Thread(run(handler_class=HttpGetHandler)).start()
		#run(handler_class=HttpGetHandler)
			
	except KeyboardInterrupt:
		hunghttpd.server_close()
		print('Error')
