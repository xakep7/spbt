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
import multiprocessing as mp
from signal import signal, SIGPIPE, SIG_DFL
from socket import error as SocketError
import errno

vers = "SPBT v0.5.0"
server_host = ''
server_port = 8050
interval = 1800
minint = 60
stime = 0
hunghttpd = None
sub_run = True
scrape_int = 120
cf_header = "X-Forwarded-For"
torrents = {}
whitelist = {}
users = {}
req_stats = {"ann":0,"scrape":0,"users":{"seaders":0,"leechers":0},"last_log":0,"last_ann":0,"start_time":0}
cgitb.enable()
signal(SIGPIPE,SIG_DFL)
last_clean = 0
background_tasks = set()

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
			if(whitelisted):
				if(get_req['info_hash'] in whitelist):
					if(whitelist[get_req['info_hash']]['type'] == 1):
						self.wfile.write(bencodepy.bencode({"failure reason":"Torrent is blacklisted by DMCA","min interval":minint}))
						return 0
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
			if('event' in get_req):
				if(get_req['event'] == "started"):
					if(get_req['left'] == 0):
						complete = True
					else:
						complete = False
				elif(get_req['event'] == "completed"):
					torrents[get_req['info_hash']]['completed'] += 1
					complete = True
				elif(get_req['event'] == "stopped"):
					if(peerhash in torrents[get_req['info_hash']]['users']):
						if(torrents[get_req['info_hash']]['users'][peerhash]['complete']):
							torrents[get_req['info_hash']]['seaders'] -= 1
							req_stats['users']['seaders'] -= 1
						else:
							torrents[get_req['info_hash']]['leechers'] -= 1
							req_stats['users']['leechers'] -= 1
						del torrents[get_req['info_hash']]['users'][peerhash]
						if(peerhash in users):
							users[peerhash]['torrs'] = remove_array_item(users[peerhash]['torrs'],get_req['info_hash'])
					self.wfile.write(bencodepy.bencode({"failure reason":"Stopped. End","min interval":minint}))
					return 0
				else:
					complete = False
			else:
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
					pass
				elif(get_req['info_hash'] in torrents and user in torrents[get_req['info_hash']]['users']):
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
			self.wfile.write(bencodepy.bencode({"interval":interval,"min interval":minint,"peers":peers,"peers6":peers_ipv6,"complete":seeds,"incomplete":leech}))
		else:
			self.wfile.write('<html><head><meta charset="utf-8">'.encode())
			self.wfile.write('<title>Nothing here.</title></head>'.encode())
			self.wfile.write('<body>Undefined action. Please see man page</body></html>'.encode())

def time_s():
	return datetime.strftime(datetime.now(), "%H:%M:%S")

def cleanup_users():
	global last_clean,users,torrents,req_stats,cleanup_int
	while sub_run:
		if(last_clean <= timestamp() - cleanup_int):
			#print(time_s(),"Cleanup Started. Users:",len(users),"torrents",len(torrents)," leech:",req_stats['users']['leechers'],"seeds:",req_stats['users']['seaders'])
			ts = tl = 0
			for user in list(users):
				if(users[user]['updated'] < int(timestamp() - interval*1.2)):
					if('torrs' in users):
						for tr in list(users[user]['torrs']):
							if(user in torrents[tr]['users']):
								if(torrents[tr]['users'][user]['complete']):
									if(torrents[tr]['seaders'] > 0):
										torrents[tr]['seaders'] -= 1
										req_stats['users']['seaders'] -= 1
								else:
									if(torrents[tr]['leechers'] > 0):
										torrents[tr]['leechers'] -= 1
										req_stats['users']['leechers'] -= 1
								del torrents[tr]['users'][user]
							if(torrents[tr]['seaders'] <= 0 and torrents[tr]['leechers'] <= 0):
								del torrents[tr]
					del users[user]
				elif(user in users):
					if(len(users[user]['torrs']) > 0):
						for tr in list(users[user]['torrs']):
							if(tr in torrents):
								if(user in torrents[tr]['users']):
									if(torrents[tr]['users'][user]['timestamp'] < timestamp() - interval*1.2):
										if(torrents[tr]['users'][user]['complete']):
											if(torrents[tr]['seaders'] > 0):
												torrents[tr]['seaders'] -= 1
												req_stats['users']['seaders'] -= 1
										else:
											if(torrents[tr]['leechers'] > 0):
												torrents[tr]['leechers'] -= 1
												req_stats['users']['leechers'] -= 1
										del torrents[tr]['users'][user]
										users[user]['torrs'] = remove_array_item(users[user]['torrs'],tr)
							else:
								if(user in users):
									users[user]['torrs'] = remove_array_item(users[user]['torrs'],tr)
									if(len(users[user]['torrs']) == 0):
										del users[user]
					else:
						del users[user]
			for tr in list(torrents):
				if(torrents[tr]['seaders'] <= 0 and torrents[tr]['leechers'] <= 0):
					del torrents[tr]
				elif(torrents[tr]['updated'] < int(timestamp() - interval*1.2)):
					if(torrents[tr]['seaders'] > 0):
						req_stats['users']['seaders'] -= torrents[tr]['seaders']
					if(torrents[tr]['leechers'] > 0):
						req_stats['users']['leechers'] -= torrents[tr]['leechers']
					del torrents[tr]
				else:
					s = l = 0
					for usr in list(torrents[tr]['users']):
						if(torrents[tr]['users'][usr]['timestamp'] < timestamp() - interval*1.2):
							del torrents[tr]['users'][usr]
							if(usr in users):
								users[usr]['torrs'] = remove_array_item(users[usr]['torrs'],tr)
								if(len(users[usr]['torrs']) == 0):
									del users[usr]
						elif(torrents[tr]['users'][usr]['complete']):
							s += 1
						else:
							l += 1
					torrents[tr]['leechers'] = l
					torrents[tr]['seaders'] = s
					ts += s
					tl += l
			req_stats['users']['leechers'] = tl
			req_stats['users']['seaders'] = ts
			print(time_s(),"Cleanup complete. Users:",len(users),"torrents",len(torrents)," leech:",req_stats['users']['leechers'],"seeds:",req_stats['users']['seaders'])
			gc.collect()
			counts = gc.get_count()
			#print(time_s(),"Cleanup garbage:",counts)
			last_clean = timestamp()
		time.sleep(1)

def logging(mysql_c,mysql_reload,mysql_loging,req_stats,torrents,users,cfg):
	reltime = int(round(mysql_reload / 60,0))
	msq_c = mysql_c(cfg.get("MYSQL","HOST"), cfg.get("MYSQL","USER"), cfg.get("MYSQL","PASSWORD"), cfg.get("MYSQL","NAME"))
	print(time_s()," Logging started")
	manager = mp.Manager()
	while sub_run:
		ts = time_s()
		ds = ts.split(':')
		if(req_stats['last_log'] <= timestamp() - mysql_reload and mysql_loging==1):
			#print("this time",reltime)
			if(int(ds[1]) % reltime == 0):
				print(time_s()," Update stats: Started")
				if(req_stats['last_log'] == 0):
					print(time_s()," Update stats: need to sync time. Aborted. Last:",req_stats['last_log'])
					req_stats['last_log'] = timestamp()
					req_stats['last_ann'] = req_stats['ann']
					req_stats['start_time'] = stime
					mysql_proc = mp.Process(target=msq_c.log,args=(req_stats,torrents,users))
					mysql_proc.start()
				else:
					mysql_proc = mp.Process(target=msq_c.log,args=(req_stats,torrents,users))
					mysql_proc.start()
					req_stats['last_log'] = timestamp()
					req_stats['last_ann'] = req_stats['ann']
					print(time_s()," Update stats: Completed")
				if(whitelisted):
					whitelist = msq_c.loadwhitelist()
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
		whitelisted = cfg.getint("OPTIONS","whitelisted")
		minint = cfg.getint("OPTIONS","announce_min")
		scrape_int = cfg.getint("OPTIONS","scrape_int")
		cf_header = cfg.get("OPTIONS","cf_header")
		mysql_loging = cfg.getint("OPTIONS","mysql_store")
		mysql_reload = cfg.getint("OPTIONS","mysql_reload")
		cleanup_int = cfg.getint("OPTIONS","cleanup_interval")
		gc.enable()
		gc.set_threshold(mysql_reload, int(mysql_reload/2), int(mysql_reload/4))
		if(mysql_loging == 1):
			print (time_s(),' Mysql connection enabled')
			from mysql_log import *
			msq = mysql_c(cfg.get("MYSQL","HOST"), cfg.get("MYSQL","USER"), cfg.get("MYSQL","PASSWORD"), cfg.get("MYSQL","NAME"))
			torrents = msq.loadtorrents()
			if(whitelisted):
				whitelist = msq.loadwhitelist()
			del msq
		else:
			print (time_s(),' Mysql connection disabled ',mysql_loging)
		print (time_s(),' Starting server at port tcp ',server_host,':', server_port," header:",cf_header)
		httpdserverthread = threading.Thread(target=run, daemon=True)
		httpdserverthread.start()
		if(mysql_loging==1):
			print(time_s()," Start logging subprocces")
			msql = threading.Thread(target=logging, args=(mysql_c,mysql_reload,mysql_loging,req_stats,torrents,users,cfg))
			msql.start()
		cleanup_thread = threading.Thread(target=cleanup_users, daemon=True)
		cleanup_thread.start()
		while True:
			in_str = input()
			if(in_str == "reload config"):
				cfg.read("tracker.cfg")
				interval = cfg.getint("OPTIONS","announce_refresh")
				minint = cfg.getint("OPTIONS","announce_min")
				scrape_int = cfg.getint("OPTIONS","scrape_int")
				whitelisted = cfg.getint("OPTIONS","whitelisted")
				cf_header = cfg.get("OPTIONS","cf_header")
				mysql_loging = cfg.getint("OPTIONS","mysql_store")
				mysql_reload = cfg.getint("OPTIONS","mysql_reload")
				cleanup_int = cfg.getint("OPTIONS","cleanup_interval")
				gc.set_threshold(mysql_reload, int(mysql_reload/2), int(mysql_reload/4))
				print("Config reloaded")
			elif(in_str == "stats"):
				print(time_s(),"Stats. Users:",len(users),"torrents",len(torrents)," eech:",req_stats['users']['leechers'],"seeds:",req_stats['users']['seaders'],"annonces:",req_stats['ann'], "scrapes:",req_stats['scrape'],"ann_req:",round((req_stats['ann']-req_stats['last_ann'])/(timestamp()-req_stats['last_log']),2))
			else:
				print("Undefined action")
		
		#threading.Thread(run(handler_class=HttpGetHandler)).start()
		#run(handler_class=HttpGetHandler)
			
	except KeyboardInterrupt:
		hunghttpd.server_close()
		sub_run = False
		cleanup_thread.join()
		msql.join()
		print('Error')
