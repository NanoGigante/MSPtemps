from serial import Serial
from serial.tools import list_ports
import paho.mqtt.client as mqtt
from datetime import datetime
from time import time

class Publisher:
    def __init__(self,MQTTserver : str, port : str = None ) -> None:
        self.BROKER = MQTTserver.split(":")[0]
        self.TOPIC = MQTTserver.split(":")[1]

        # se la porta non è specificata cerca il nome msp nelle descrizioni delle porte seriali connesse
        # non ho testato se va anche su linux
        if port:
            self.port = port
        else:
            for port in list_ports.comports():
                if port.device:
                    if "MSP Application UART" in port.description:
                        self.port = port.device
        #se ancora non ha trovato una porta ritorna
        if not port:
            raise ValueError("Specificare una porta seriale")
        
        self.MQTTclient = mqtt.Client()

        #funzioni mqtt
        def on_connect(client, userdata, flags, rc):
            print(f'{mqtt.connack_string(rc)}')

        def on_publish(client, userdata, mid):
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] msg published with id: {mid}')

        #events --> callback association
        self.MQTTclient.on_connect = on_connect 
        self.MQTTclient.on_publish = on_publish

        #self.MQTTclient --> broker connection
        print('MQTT client connecting...', end = ' ')
        self.MQTTclient.connect(self.BROKER)
        self.MQTTclient.loop_start()
    
    def start_reading(self,readlimit : int = None):
        print(f'reading on port {self.port}\npublishing on {self.BROKER} on topic {self.TOPIC}')
        sp = Serial(self.port)
        

        while True:
            try:
                temp = float(sp.readline().decode("UTF-8").strip()) / 10

                self.MQTTclient.publish(self.TOPIC,f'{temp}')
            except KeyboardInterrupt:
                sp.close()
                self.MQTTclient.loop_stop()
                self.MQTTclient.disconnect()
                return
            except (UnicodeDecodeError, ValueError):
                continue
        


if __name__ == '__main__':
    a = Publisher("192.168.178.48:MSPtemps")
    a.start_reading()