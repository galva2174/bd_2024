#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json


topic=sys.argv[1]
comments={}
consumer=KafkaConsumer(topic, value_deserializer = lambda m: json.loads(m.decode('ascii')))
for message in consumer:
	if "stop" in message.value:
		break
	if message.value[0] in comments:
		comments[message.value[0]].append(message.value[-1])
	else:
		comments[message.value[0]]=[message.value[-1]]
	#print(message.value)
	#print(comments)
comments=dict(sorted(comments.items()))
print(json.dumps(comments, indent=4))
consumer.close()
