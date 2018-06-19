import os
import pandas as pd
import numpy as np
import argparse
import shutil
import sys

#for fetching 
sys.path.append(os.path.abspath('../config/setting'))

import Settings
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

class LogProducer:
	stn = Settings.Settings
	Products = []
	Referrers = []

	#present working directory
	dir_path = os.path.dirname(os.path.realpath(__file__))
	print(dir_path)
	
	with open('../../resources/products.csv') as f:
    		Products = [line.rstrip() for line in f.readlines()]

	with open('../../resources/referrers.csv') as f:
		Referrers = [line.rstrip() for line in f.readlines()]
	
	Visitors = ["Visitor-"+str(no) for no in list(range(stn.visitors))]
	Pages = ["Page-"+str(no) for no in list(range(stn.pages))]

	filePath = stn.filePath	
	destPath = stn.destPath
	incrementTimeEvery = random.randint(0,(min(stn.records, 100) - 1) + 1)
	
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	topic = stn.kafkaTopic

	for count in range(stn.no_of_files):
		with open(filePath,'a') as f:
		
			f.write("timestamp_hour\treferrer\taction\tprevPage\tvisitor\tpage\tproduct\n")
			timestamp = int(round(time.time() * 1000))
			adjustedTimestamp = timestamp
	
			for iteration in range(stn.records):
				adjustedTimestamp = adjustedTimestamp + ((int(round(time.time() * 1000)) - timestamp) * stn.timeMultiplier)
				timestamp = int(round(time.time() * 1000))

				if iteration % (random.randint(0,200) + 1) == 0:
					action = "purchas"
				elif iteration % (random.randint(0,200) + 1) == 1:
					action = "add_to_cart"
				else:
					action = "page_view"

				referrer = Referrers[random.randint(0,len(Referrers) - 1)]
				if referrer == "Internal":
					prevPage = Pages[random.randint(0,len(Pages) - 1)]
				else:
					prevPage = ""
		
				visitor = Visitors[random.randint(0,len(Visitors) - 1)]
				page = Pages[random.randint(0,len(Pages) - 1)]
				product = Products[random.randint(0,len(Products) - 1)]

				line = str(adjustedTimestamp)+"\t"+str(referrer)+"\t"+str(action)+"\t"+str(prevPage)+"\t"+str(visitor)+"\t"+str(page)+"\t"+str(product)
				line_to_kafka = line.encode('utf-8')
				producer.send(topic, line_to_kafka)
				f.write(line+"\n")
		
				if iteration % incrementTimeEvery == 0:
					print("Sent "+str(iteration)+" messages!")
					sleeping = 2
					#time.sleep(2)
					print("Sleeping for "+str(sleeping)+" ms")
			f.close()
				
			outFile = destPath + "data_" + str(timestamp)
			print("Moving file to "+outFile)
			shutil.move(filePath,outFile)
			sleeping = 2
			time.sleep(sleeping)
			print("Sleeping for "+str(sleeping)+"ms")