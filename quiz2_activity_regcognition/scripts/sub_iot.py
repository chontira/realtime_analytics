import microgear.client as client
from confluent_kafka import Producer
import logging
import time

appid = "ekaratnida"
gearkey = 'jtD9ag08syPtqiK' # key
gearsecret = 'vDEEIuw9Ssj4OvbrBHmM4hZfa' # secret

client.create(gearkey,gearsecret,appid,{'debugmode': True}) # สร้างข้อมูลสำหรับใช้เชื่อมต่อ
client.setalias("ekarat") # ตั้งชื่้อ

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})

def callback_connect() :
    print ("Now I am connected with netpie")
    
def callback_message(topic, message) :
    print(topic, ": ", message)
    # ส่งข้อมูลจาก netpie ไป kafka ตรงนี้ ใช้ producer-local.py ใน week3
    p.poll(0)
    # ส่งข้อมูลต่อไป kafka topic ให้ spark streaming มา process ต่อ
    p.produce('accelerometer-input', message.encode('utf-8'), callback=delivery_report)

def callback_error(msg) :
    print("error", msg)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass

client.on_connect = callback_connect # แสดงข้อความเมื่อเชื่อมต่อกับ netpie สำเร็จ
client.on_message= callback_message # ให้ทำการแสดงข้อความที่ส่งมาให้
client.on_error = callback_error # หากมีข้อผิดพลาดให้แสดง
client.subscribe("/bads") # ชื่อช่องทางส่งข้อมูล ต้องมี / นำหน้า และต้องใช้ช่องทางเดียวกันจึงจะรับส่งข้อมูลระหว่างกันได้
client.connect(True) # เชื่อมต่อ ถ้าใช้ True เป็นการค้างการเชื่อมต่อclient.on_message= callback_message # ให้ทำการแสดงข้อความที่ส่งมาให้