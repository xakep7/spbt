# spbt
Simple Python Bittorrent Tracker (http only at this time)

# Requirements
- Python >= 3.7
- Python extensions: mysql-connector-python,configparser,bencode.py,hashlib,urllib,threading,struct,netaddr,socket,http.server,cgitb,argparse,datetime,random,ipaddress,signal

# Features
- Http announce and scrape
- MySQLDB logging and stats (optional)
- Threading. All request is async.
- High perfomance. Little bit more resource consumption than XBT at same load
- UDP proto coming soon.

This tracker software tested at spec:
- E3-1230v6 @3.50 Ghz
- 16 Gb DDR4 2133 Mhz
- 2x450 Gb NVMe PCI-e v3
100000+ torrents and users. 580-660 req/s. Cpu consuption - 5-10%, Memory - 3-6%
Code may has some shitcoding. Alpha.
