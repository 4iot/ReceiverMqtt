# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import time
import json
import pymongo
import hashlib
import time
import re
import ConfigParser

global msgQueueDir

# grant_token: returns the token. Please take your wonderful  funtion
def grant_token(Requester):
	timeSec = int(round(time.time() * 1000))
	timeMs = timeSec / 3600000
	Requester = Requester + str(timeMs)
	rev = 'a'
	__token = hashlib.md5(Requester).hexdigest() + 'a'
	return __token


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
	print("Connected with result code "+str(rc))
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	client.subscribe("4iot/#",2)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	if ( msg.topic == "4iot/message" ):
		message = json.loads(msg.payload)
		print "Topic: ", msg.topic+"\nMessage: "+ msg.payload
		
		# iotdb.messages.insert(message)
		a = 1
		# write in /Queue/filename
      #filename = id + "-" + System.currentTimeMillis() + "-" + this.clientIp + '-' + request.getRemotePort() + ".himessage";
		filename = msgQueueDir + '/' + message['id'] + '-' + str(int(round(time.time() * 1000))) + '-' + message['clientIp'] + '.himessage'
		FH = open(filename,'w')
		FH.write(json.dumps(message))
		FH.close
	elif ( msg.topic == "4iot/please_register" ):
		# register claim from new client
		requester = str(msg.payload)
		print "this is a register request from " + requester
		Token = grant_token (requester)
		grant_topic = "4iot/register_accepted/" + requester
		grant_response = "device is now identified as [" + Token + "]"
		client.publish(grant_topic,grant_response,2)
	elif ( msg.topic == "4iot/assign_token/" ):
		match = re.search('device is now identified as \[(\w{33})',the_page)
		

Config = ConfigParser.ConfigParser()
CfgFilename = './OpenMessageReceiver.ini'
Config.read(CfgFilename)
mqttBroker = Config.get('mqtt','broker',raw=False)
mqttPort = Config.getint('mqtt','port')
mqttKeepalive = Config.getint('mqtt','keepalive')

mongoHostname = Config.get('mongodb','hostname',raw=False)
mongoPort = Config.get('mongodb','port')

msgQueueDir = Config.get('misc','queue_dir',raw=False)

# conn=pymongo.MongoClient('158.69.160.58',27017)
# iotdb=conn.iot_staging

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqttBroker, mqttPort, mqttKeepalive)
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
