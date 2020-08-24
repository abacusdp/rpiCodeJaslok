import random
import os
import time
import paho.mqtt.client as mqtt
import json
import datetime
import traceback
from threading import Thread
import requests

import re,uuid
macAddr=''.join(re.findall('..', '%012x' % uuid.getnode())).upper()
internetConnectionFlag=True 



broker_url  = "172.16.50.15"
client_id="randomClientId"+str(random.random())
broker_port = 1883
topic = "rpi_Active_Status"

client = mqtt.Client(client_id=client_id, clean_session=True)
client.username_pw_set(username="clevercare",password="sacC2p7rFaLj")

def on_connect(client, userdata, flags, rc):
   global internetConnectionFlag
   internetConnectionFlag=False
   print("Connected With Result Code ",str(rc))
   f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
   f.write("------ Mqtt connected success with "+str(rc)+" "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
   f.close()


def on_disconnect(client, userdata, rc):
   print("Client Got Disconnected")
   try:
      client.reconnect()
      f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
      f.write("------ Mqtt Disconnected Success "+str(rc)+" "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
      f.close()
   except Exception as e:
      print("Reconnect Error")
      f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
      f.write(str(e)+" in Reconnect at  "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"\n")
      f.close()
      if str(err)=="[Errno 32] Broken pipe" :
         os.system("sudo reboot")


def checkConnection():
    try:
        if requests.get('https://google.com').ok:
            return True
    except Exception as err:
        return False







msg={}



class ActiveClass(Thread):
    def run(self):
        while True:
            time.sleep(5)
            try:
                msg={"status":"ok","mac":macAddr,"time":int(round(time.time()*1000)) }
                client.publish(topic, payload=json.dumps(msg), qos=0, retain=False)
            except Exception as e:
                f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
                f.write(str(e)+" in publish at  "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"\n")
                f.close()
                if str(err)=="[Errno 32] Broken pipe":
                   os.system("sudo reboot")

ActiveClassobj = ActiveClass()


time.sleep(5)
client.on_connect = on_connect
client.on_disconnect = on_disconnect

try:
    client.connect(broker_url, broker_port,keepalive=120)
    if checkConnection():
        time.sleep(5)
        ActiveClassobj.start()
except Exception as err:
    print(err)
    f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
    f.write("------ Network issue "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
    f.close()


class connectMqtt(Thread):
    def run(self):
        while True:
            global internetConnectionFlag
            time.sleep(5)
            if internetConnectionFlag:
                if checkConnection():
                    internetConnectionFlag=False
                    client.reconnect()
                    ActiveClassobj.start()




objConnectMqtt=connectMqtt()
objConnectMqtt.start()
client.loop_start()
