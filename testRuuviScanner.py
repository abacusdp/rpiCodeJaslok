# Version 1.0 Updated Date : 24/08/2020 Name : DataStatus
import random
import os
import time
import paho.mqtt.client as mqtt
import json
import datetime
import traceback
from threading import Thread
from ruuvitag_sensor.adapters.nix_hci import BleCommunicationNix
import ruuvitag_sensor.log
from ruuvitag_sensor.data_formats import DataFormats
from ruuvitag_sensor.decoder import get_decoder
import requests



import re,uuid
macAddr=''.join(re.findall('..', '%012x' % uuid.getnode())).upper()
internetConnectionFlag=True


ruuvitag_sensor.log.enable_console()
ble = BleCommunicationNix()
publish_array=[]
broker_url  = "172.16.50.15"
client_id="randomClientId"+str(random.random())
broker_port = 1883
topic = "location_data"


client = mqtt.Client(client_id=client_id, clean_session=True)
client.username_pw_set(username="clevercare",password="sacC2p7rFaLj")

def on_connect(client, userdata, flags, rc):
   global internetConnectionFlag
   internetConnectionFlag=False
   print("Connected With Result Code ",str(rc))
   f = open("/home/pi/Desktop/beacon_scan/ruuviScannerExceptionLogs.txt","a")
   f.write("------ Mqtt connected success with "+str(rc)+" "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
   f.close()

def on_disconnect(client, userdata, rc):
   print("Client Got Disconnected")
   try:
      client.reconnect()
      f = open("/home/pi/Desktop/beacon_scan/ruuviScannerExceptionLogs.txt","a")
      f.write("------ Mqtt Disconnected Success "+str(rc)+" "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
      f.close()
   except Exception as e:
      print("Reconnect Error")
      f = open("/home/pi/Desktop/beacon_scan/ruuviScannerExceptionLogs.txt","a")
      f.write(str(e)+" in mqttDisconnected at  "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"\n")
      f.close()
      if str(err)=="[Errno 32] Broken pipe" :
         os.system("sudo reboot")



client.on_connect = on_connect
client.on_disconnect = on_disconnect

print("scanning starting in 5 Seconds")
time.sleep(5)




def checkConnection():
    try:
        if requests.get('https://google.com').ok:
            return True
    except Exception as err:
        return False

def giveBatteryPercentage(mvolts):
   if mvolts >= 3000:
      battery_level = 100
   elif (mvolts > 2900):
      battery_level = 100 - ((3000 - mvolts) * 58) / 100
   elif (mvolts > 2740):
      battery_level = 42 - ((2900 - mvolts) * 24) / 160
   elif (mvolts > 2440):
      battery_level = 18 - ((2740 - mvolts) * 12) / 300
   elif (mvolts > 2100):
      battery_level = 6 - ((2440 - mvolts) * 6) / 340
   else:
      battery_level = 0
   return battery_level

def generateLowCriticalBatteryAlert(obj):
   msg={}
   if obj["battery"]<85:
      msg={"message":"Critical Battery Alert","mac":obj["mac"].upper(),"batteryPer":obj["battery"]}
   elif obj["battery"]<95:
      msg={"message":"Low Battery Alert","mac":obj["mac"].upper(),"batteryPer":obj["battery"]}
   client.publish("topic_LowCriticalBattery", payload=json.dumps(msg), qos=0, retain=False)


macAccelerometerMovementObject={}
def generareAccelerometerAlert(obj):
   global macAccelerometerMovementObject
   if macAccelerometerMovementObject.__contains__(obj["mac"].upper()):

      if (macAccelerometerMovementObject[obj["mac"].upper()]["movement_counter"]>=obj["movement_counter"]-2 or macAccelerometerMovementObject[obj["mac"].upper()]["movement_counter"]==obj["movement_counter"] ) and macAccelerometerMovementObject[obj["mac"].upper()]["updated_time"]-macAccelerometerMovementObject[obj["mac"].upper()]["start_time"]>=300000:

         client.publish("topic_Accelerometer", payload=json.dumps({"message":"Accelerometer Alert","mac":obj["mac"],"movement_counter":obj["movement_counter"]  }), qos=0, retain=False)

         macAccelerometerMovementObject[obj["mac"].upper()]["start_time"]= obj["timestamp"]
      else:
         macAccelerometerMovementObject[obj["mac"].upper()]["movement_counter"]=obj["movement_counter"]
         macAccelerometerMovementObject[obj["mac"].upper()]["updated_time"]=obj["timestamp"]
   else:
      macAccelerometerMovementObject[obj["mac"].upper()]={ "movement_counter":obj["movement_counter"],"start_time":obj["timestamp"],"updated_time":obj["timestamp"]  }





class publishMessageToMqtt(Thread):
   def run(self):
      while True:
         global publish_array
         time.sleep(1)

         try:

            print(str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
            wall_device_obj={
               "timestamp":str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")),
               "type":"Gateway",
               "mac":macAddr, #1->AC233FC017A2 2->AC233FC017A2  3->AC233FC017A3 4->
               "gatewayFree":91,
               "gatewayLoad":0.17
            }

            publish_array.insert(0,wall_device_obj)

            client.publish(topic, payload=json.dumps(publish_array), qos=0, retain=False)#publishing to topic_test7

            publish_array=[]
         except Exception as e:
            f = open("/home/pi/Desktop/beacon_scan/ruuviScannerExceptionLogs.txt","a")
            f.write(str(e)+" in publish at  "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"\n")
            f.close()
            print(str(e))
            if str(err)=="[Errno 32] Broken pipe":
               os.system("sudo reboot")



publishMqttobj = publishMessageToMqtt()



class connectMqtt(Thread):
   def run(self):
      global internetConnectionFlag
      while True:
         global internetConnectionFlag
         time.sleep(5)

         if internetConnectionFlag:
            if checkConnection():
               internetConnectionFlag=False
               client.reconnect()

               publishMqttobj.start()


         


try:
    client.connect(broker_url, broker_port,keepalive=120)
    if checkConnection():
        time.sleep(5)
        publishMqttobj.start()


except Exception as err:
    f = open("/home/pi/Desktop/beacon_scan/activeRpiStatusException.txt","a")
    f.write("------ Network issue "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"-----\n")
    f.close()


objConnectMqtt=connectMqtt()
objConnectMqtt.start()
client.loop_start()



for ble_data in ble.get_datas():
   try:
      if(len(ble_data[1])==66):
         (data_format, encoded) = DataFormats.convert_data(ble_data[1])
         if not (data_format==None or encoded==None):
            sensor_data = get_decoder(data_format).decode_data(encoded)
            sensor_data["rssi"]=int(ble_data[1][-2:],16)-256
            sensor_data["battery"]=round(giveBatteryPercentage(sensor_data["battery"]),2)
            sensor_data["timestamp"]=int(datetime.datetime.now().timestamp()*1000)

            publish_array.append(sensor_data)

            generateLowCriticalBatteryAlert(sensor_data)
            generareAccelerometerAlert(sensor_data)



   except Exception as err:
      f = open("/home/pi/Desktop/beacon_scan/ruuviScannerExceptionLogs.txt","a")
      f.write(str(err)+" in scanning at  "+str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))+"\n")
      f.close()
      client.reconnect()
      if str(err)=="[Errno 32] Broken pipe":
         os.system("sudo reboot")
