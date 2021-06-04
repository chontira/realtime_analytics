#!/bin/bash

cd /Users/new/Documents/BADS7205_Realtime/quiz2_activity_regcognition
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 activity_recognition.py