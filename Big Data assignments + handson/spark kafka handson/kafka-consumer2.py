#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json

topic=sys.argv[1]
likes={}
consumer=KafkaConsumer(topic, value_deserializer = lambda m: json.loads(m.decode('ascii')))
for message in consumer:
	if "stop" in message.value:
		break
	user=message.value[0]
	post=message.value[1]
	if user not in likes:
		likes[user]={}
	if post in likes[user]:
		likes[user][post]+=1
	else:
		likes[user][post]=1
	#print(message.value)
likes=dict(sorted(likes.items()))
print(json.dumps(likes, indent=4))
consumer.close()
