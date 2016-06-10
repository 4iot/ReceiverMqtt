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
	topicElems = msg.topic.split('/')
	#if ( msg.topic == "4iot/message" ):
	if ( topicElems[1] == 'insert' ):
		stmt = topicElems[1]
		tabname = topicElems[2]
		message = json.loads(msg.payload)
		print stmt+ " into " + tabname  +"\nContents: "+ msg.payload
		filename = msgQueueDir + '/' + tabname + '-' + stmt + '-' + message['id'] + '-' + str(int(round(time.time() * 1000))) + '-' + message['clientIp'] + '.himessage'
		if ( mongoConnect == True ):
			# Build the query dynamically
			statement = "iotdb." + tabname + "." + stmt + "(message)"
			try:
				eval(statement)
			except pymongo.errors.OperationFailure as m2:
				print ('Device ' +  'Could not insert iot message' )
				FH = open(filename,'w')
				FH.write(json.dumps(msg.payload))
				FH.close
		else:
			# write in /Queue/filename
			#filename = id + "-" + System.currentTimeMillis() + "-" + this.clientIp + '-' + request.getRemotePort() + ".himessage";
			FH = open(filename,'w')
			FH.write(json.dumps(msg.payload))
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

try:
    mongoHostname = Config.get('mongodb','hostname',raw=False)
    mongoPort = Config.getint('mongodb','port')
    mongoPort = int(mongoPort)
    mongoConnect = True
except:
    mongoHostname = None
    mongoPort = None
    mongoDbname = None
    mongoConnect = False
    pass

if ( mongoConnect == True ):
    try:
        mongoDbname = Config.get('mongodb','dbname')
    except:
        mongoDbname = 'iot_staging'
        pass

msgQueueDir = Config.get('misc','queue_dir',raw=False)

if ( mongoConnect == True ):
	connString = 'mongodb://' + mongoHostname + ':' + str(mongoPort) + '/'
	try:
		conn=pymongo.MongoClient(connString)
		#conn=pymongo.MongoClient('158.69.160.58',27017)
		openDb = 'conn.' + mongoDbname
    		#iotdb=conn.iot_staging
    		iotdb=openDb
		#eval (openDb)

	except pymongo.errors.OperationFailure as m1:
		print ('Cannot connect to mongo instance on ' + mongoHostname )
		mongoConnect = False

	if ( mongoConnect == True ):
		#connectString = 'iotdb=conn.' + mongoDbname
		#connectString = 'iotdb=conn.iot_staging'
		#try:
			#eval(connectString)
			#mongoConnect = True
		#except pymongo.errors.OperationFailure as m2:
			#print ('Cannot connect to mongo database ' + mongoDbname )
			#mongoConnect = False
		iotdb=conn.iot_staging

client = mqtt.Client("4iotReceiver")
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqttBroker, mqttPort, mqttKeepalive)
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
