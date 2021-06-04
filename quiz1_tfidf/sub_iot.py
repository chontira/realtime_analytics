import microgear.client as client
import logging
import time

appid = "ekaratnida"
gearkey = 'mdOVyTkRngN6tAP' # key
gearsecret = 'Dd69q8KOZIXvbs0N758cenwha' # secret

client.create(gearkey,gearsecret,appid,{'debugmode': True}) # สร้างข้อมูลสำหรับใช้เชื่อมต่อ

client.setalias("Chontira") # ตั้งชื่้ออุปกรณ์

def callback_connect() :  # เมื่อ connect ให้ทำอะไร
    print ("Now I am connected with netpie")
    
def callback_message(topic, message) :  # เมื่อมี message เข้ามาให้ print topic กับ message
    print(topic, ": ", message)

def callback_error(msg) : # เมื่อมี error เข้ามา print error
    print("error", msg)

client.on_connect = callback_connect # แสดงข้อความเมื่อเชื่อมต่อกับ netpie สำเร็จ
client.on_message= callback_message # ให้ทำการแสดงข้อความที่ส่งมาให้
client.on_error = callback_error # หากมีข้อผิดพลาดให้แสดง
client.subscribe("/scores/#") # ชื่อช่องทางส่งข้อมูล ต้องมี / นำหน้า และต้องใช้ช่องทางเดียวกันจึงจะรับส่งข้อมูลระหว่างกันได้
client.connect(True) # เชื่อมต่อ ถ้าใช้ True เป็นการค้างการเชื่อมต่อclient.on_message= callback_message # ให้ทำการแสดงข้อความที่ส่งมาให้