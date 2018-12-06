#!/usr/bin/python3

import socket
import time
import json
import datetime
import paho.mqtt.client as mqtt
 
socket_host = "www.google.com"
socket_port = 9808

mqtt_transport = 0
mqtt_ip = ''
mqtt_topic = ''
mqtt_qos = 0
#publisher 9808 sub 9807
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((socket_host, socket_port))
client.settimeout(None)

while (mqtt_transport == 0):
    client.send(("mqtt").encode())
    client.settimeout(None)
    response = client.recv(4096).decode('utf-8')
    try:
        socket_rec = response.loads(response)
        mqtt_ip = socket_rec['broker']
        mqtt_topic = socket_rec['topic']
        mqtt_qos = socket_rec['qos']
        mqtt_transport = 1
    except:
        mqtt_transport = 0

try:
    mqtt_conn = mqtt.Client()
    mqtt_conn.connect(mqtt_ip, 1883)
    client.send('{"status":"create"}').encode()
except:
    client.send('{"status":"error"}')
    mqtt_transport = 0

while(mqtt_transport == 1):
    data_check = 0
    print("-----------------------------------------------------------")
    response = client.recv(4096).decode('utf-8')
    print("socket data : ", response)
    print("-----------------------------------------------------------")
    try:
        socket_rec = json.loads(response)
        data = socket_rec['recv']
        print("json data = ", data)
        data_check = 1
    except:
        lient.send('{"mqtt_send":"data_error"}').encode()
        print("json data error")

    if(data_check == 1):
        try:
            mqtt_conn.connect(mqtt_ip, 1883)
            mqtt_conn.publish(mqtt_topic, response, mqtt_qos)
            now = datetime.datetime.now()
            print('MQTT To Server OK ! -->' , now)
            client.send('{"mqtt_send":"ok"}').encode()
        except:
            print('MQTT To Server Error ! -->' , now)
            client.send('{"mqtt_send":"broker_error"}').encode()
    else:
        pass
    print("-----------------------------------------------------------")
    time.sleep(3)