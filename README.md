# spbt
Simple Python Bittorrent Tracker (http only at this time)

# Requirements
- Python >= 3.7
- Python extensions:
  - mysql-connector-python
  - bencode.py
  - netaddr

# Features
- Http announce and scrape
- Supports IPv6. Compact mode or none.
- Supports IP from http header. Nginx (X-Forwarded-For) or Cloudflare (CF-Forwarded-for). cf_header setting.
- MySQLDB logging and stats (optional)
- Cleanup torrents and users for memory safe.
- Threading. All request is async.
- High perfomance. Little bit more resource consumption than XBT at same load
- UDP proto coming soon.
- DMCA blacklist/whitelist.

# Path
- /stats - server statistics json output
- /announce - main announce path
- /scrape - srapper path

# CLI Commands
- stats - Print tracker stats
- reload config - reloads config file and accept new vars

# Test Tracker Hardware
This tracker software tested at spec:
- E3-1230v6 @3.50 Ghz
- 16 Gb DDR4 2133 Mhz
- 2x450 Gb NVMe PCI-e v3
- 100k+ torrents and users. 580-700 announce req/s. Cpu consuption - 5-10%, Memory - 3-6%
  
test tracker stats: https://tr2.trkb.ru/index.php?page=tracker  
Code may has some shitcoding. Alpha.
