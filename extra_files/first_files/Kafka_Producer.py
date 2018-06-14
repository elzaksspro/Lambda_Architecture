import time
import os
import pandas as pd
import numpy as np
import argparse
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError


def simulator_data(datafile):
    '''
    takes data file and publishes to topic named 'sensor' example usage: python KafkaProducer.py data.txt
    '''
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = "sensor"

    #opening the data file and sending data line by line through the kafka producer --simulating the production of live data

    index = 0
    with open(datafile) as file:
        for line in file:
            index += 1
            senddata = line.encode('utf-8')
            producer.send(topic, senddata)

            if index == 100:
                index = 0
                time.sleep(5)


if __name__ == '__main__':
    filename = input('Enter the filename : ')
    simulator_data(filename)
