
import paho.mqtt.client as mqtt
from datetime import datetime
import json
from time import time,sleep
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Sub:

	def __init__(self,BROKER,TOPIC,Influx_cfgfile) -> None:	
		self.InfluxInit(Influx_cfgfile) #inizializza influx
		self.MQTTInit(BROKER,TOPIC) #inizializza mqtt
		
	#INFLUX INIT
	def InfluxInit(self,configfile : str):
		#carica le credenziali dal file json
		try:
			with open(configfile) as creds_file:
				creds = json.load(creds_file)
		except:
			raise ValueError("il file di config non esiste o non è in formato json")

		self.bucket = creds["bucket"]
		self.org = creds["org"]
		#apre la sessione
		self.session = InfluxDBClient(url=creds["url"], token=creds["token"], org=creds["org"])
	
	#MQTT INIT
	def MQTTInit(self,BROKER,TOPIC):	
		self.BROKER = BROKER
		self.TOPIC = TOPIC

		#funzioni da date a mqtt, sono dichiara
		def on_connect(client, userdata, flags, rc):
			print(f'{mqtt.connack_string(rc)}')
			print('MQTT client subscribing...', end = ' ')
			client.subscribe(self.TOPIC)

		def on_subscribe(client, userdata, mid, granted_qos):
			print(f'subscribed {self.TOPIC} with QoS: {granted_qos[0]}\n')

		################
		## ON MESSAGE ##
		################
		def on_message(client, userdata, msg):
			val = float(msg.payload.decode("utf-8"))

			record = {
                    "measurement": "temperatures",
                    "tags": {
						"device": "MSP430",
						"group" : "ferioli-malmusi"
					},
                    "fields": {
						"temp": val
					},
                }
			
			try:
				writeapi = self.session.write_api(write_options=SYNCHRONOUS)
				writeapi.write(self.bucket,self.org,record)
				print(f'message recived and forwarded to InfluxDB [temp: {val} °C]')
			except:
				print('InfluxDB error, stopping program...')
				self.client.loop_stop()
			


		self.client = mqtt.Client()
		#events --> callback association

		self.client.on_connect = on_connect
		self.client.on_subscribe = on_subscribe
		self.client.on_message = on_message

		#client --> self.BROKER connection
		print('MQTT client connecting...', end = ' ')
		self.client.connect(self.BROKER)
		

	def start(self):
		#wait and listen for events (ctrl-c to quit)
		try:
			self.client.loop_forever()
		except KeyboardInterrupt:
			print('\nMQTT client disconnecting...bye')
		finally:
			self.client.disconnect()

if __name__ == '__main__':
	a = Sub('192.168.178.48','MSPtemps','config.json')
	a.start()

	print("aaaa")

