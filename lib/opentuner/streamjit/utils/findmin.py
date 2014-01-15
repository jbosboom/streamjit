import sqlite3
import time
import os
import subprocess
while True:
	print "\n*********************************************"
	subprocess.Popen(["java", "-version"])
	print os.path.realpath(__file__)
	if os.path.isfile("sjChannelVocoder 4, 64.db"):
		print "file is accesible"
	else:
		print "OOOOOOOOOOOOPPPPPPPPSSSSSSS"
	print sqlite3.version
	print sqlite3.version_info
	sqlite3.sqlite_version_info
	with sqlite3.connect("sjChannelVocoder 4, 64.db") as conn:
		cur = conn.cursor()
		for row in cur.execute("SELECT * FROM results ORDER BY Exectime"):
			r = row[0]
			t = row[3]
			if t > 0:
				print t
				print r
				break
	time.sleep(2)
	
