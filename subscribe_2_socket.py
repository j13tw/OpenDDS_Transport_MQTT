#!/usr/bin/python3

import socket
import time
import json
import datetime
import paho.mqtt.client as mqtt
 
socket_host = "www.google.com"
socket_port = 9807

mqtt_transport = 0
mqtt_ip = ''
mqtt_topic = ''
mqtt_qos = 0

#publisher 9808 sub 9807
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((socket_host, socket_port))
client.settimeout(None)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    mqtt_sub.subscribe(mqtt_topic, mqtt_qos)

def on_message(client, userdata, message):
    now = datetime.datetime.now()
    year = str(now.year)
    month = str(now.month)
    if (int(month) < 10): month = "0" + str(month)
    day = str(now.day)
    if (int(day) < 10): day = "0" + str(day)
    hour = str(now.hour)
    if (int(hour) < 10): hour = "0" + str(hour)
    minute = str(now.minute)
    if (int(minute) < 10): minute = "0" + str(minute)
    second = str(now.second)
    if (int(second) < 10): second = "0" + str(second)
    micro_second = str(int(datetime.datetime.now().microsecond/100))
    print('------------------------------------------------------')
    print("message received -->" ,message.payload.decode('utf-8'))
    print("message topic =",message.topic)
    client.send(message.payload.decode('utf-8')).encode()
    client.send('{"mqtt_recv":"ok"}').encode()

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
    error = 0
    # MQTT connection
    mqtt_sub = mqtt.Client()
    mqtt_sub.on_message = on_message
    mqtt_sub.on_connect = on_connect
    try:
        mqtt_sub.connect(mqtt_ip, 1883)
    except:
        client.send('{"mqtt_recv":"broker_error"}').encode()
        error = 1
    if (not error):
        mqtt_sub.loop_start()
        time.sleep(1)
        mqtt_sub.loop_stop()
'''
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
        client.send('{"mqtt_send":"data_error"}').encode()
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
'''