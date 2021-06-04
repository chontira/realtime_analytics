from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

file1 = open("Book 1 - The Philosopher's Stone.txt", 'r') 
Lines = file1.readlines()
i=0
for data in Lines:

    p.poll(0)
    
    p.produce('streams-plaintext-input', data.encode('utf-8'), callback=delivery_report)
    i+=1
    if (i%1000==0):
        time.sleep(1)

p.flush()