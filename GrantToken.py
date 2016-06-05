# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import time
import pymongo


# grant_token: returns the token. Please take your wonderful  funtion
def grant_token():
	__token = "[t23456789012345678901234567890123]"
	return __token


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
	print("Connected with result code "+str(rc))
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	client.subscribe("4iot/please_grant_token/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	if ( msg.topic == "4iot/please_grant_token" )
		message = json.loads(msg.payload)
		print "Topic: ", msg.topic+"\nMessage: "+json.dumps(msg.payload)
		#print "Topic: ", msg.topic+"\nMessage: "+ message
		iotdb.messages.insert(message)
		Token = grant_token ()
		grant_topic = "4iot/assign_token/" + requester
		grant_response = "device is now identified as " + Token
		client.publish(grant_topic,grant_response)
	
		

conn=pymongo.MongoClient('158.69.160.58',27017):
iotdb=conn.iot_staging
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.1.30", 8883, 60)
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
