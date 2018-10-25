#!/usr/bin/env python

import json
from pprint import pprint
import sys
import paho.mqtt.client as mqtt_client
import coloredlogs, logging
from influxdb import InfluxDBClient
import time

conf = {}
topics = {}     # topic: details

payloads = {}   # topic: payload
last_write = {} # topic: last_write

mqtt = None
influx = None

######################### MQTT Callbacks #########################

def on_connect(mqtt, userdata, flags, result):
    if result is not mqtt_client.MQTT_ERR_SUCCESS:
        logging.error('Error connecting to MQTT server (res={})'.format(result))
        mqtt.disconnect()
        return
    logging.info('Connected as {}'.format(mqtt._client_id))
    for key in topics:
        mqtt.subscribe(key)

def on_disconnect(mqtt, userdata, rc):
    logging.info('Disconnected')

def on_message(mqtt, userdata, message):
    payloads[message.topic] = message.payload

#################### Local ####################

def process_pool():
    for topic, payload in payloads.items():
        if topics[topic]['momentary'] == True or topic not in last_write or time.time() - last_write[topic] > conf['min_write_interval']:
            last_write[topic] = time.time()
            influx_write(topic, payload)
            del payloads[topic]

def influx_write(topic, payload):
    logging.debug('Writing to influx: {} = {}'.format(topic, payload))
    topic_info = topics[topic]

    if topic_info['type'] == 'string': value = payload
    elif topic_info['type'] == 'int': value = int(payload)
    elif topic_info['type'] == 'bool': value = bool(payload != '0')
    else: value = float(payload)

    data = {
        "measurement": topic_info['measurement'],
        "tags": topic_info['tags'],
        "fields": {topic_info['field']: value}
    }
    influx.write_points([data])

def load_config():
    global conf, topics, min_write_interval, database
    logging.info('Loading config...')

    with open('logger.conf') as conf_file:
        conf = json.load(conf_file)

    for mes in conf['measurements']:
        for field in mes['fields']:
            topic = {}
            topic['measurement'] = mes['name']
            if 'tags' in mes:
                topic['tags'] = mes['tags']
            else:
                topic['tags'] = {}
            topic['field'] = field['name']
            if 'type' in field:
                topic['type'] = field['type']
            else:
                topic['type'] = 'float'
            if 'momentary' in field:
                topic['momentary'] = field['momentary']
            else:
                topic['momentary'] = False
            topics[field['topic']] = topic

def main():
    global mqtt, influx
    coloredlogs.install(level=logging.INFO, fmt='%(asctime)s %(levelname)s %(message)s')

    print('<<< Influx MQTT Logger >>>\n')

    load_config()
    # pprint(topics)
    # pprint(conf)

    logging.info('Connecting to Influx...')
    influx = InfluxDBClient('localhost', 8086, 'root', 'root', conf['influx_database'])
    influx.create_database(conf['influx_database'])

    logging.info('MQTT connecting to {}:{}...'.format(conf['mqtt_server'], conf['mqtt_port']))
    mqtt = mqtt_client.Client(conf['mqtt_client_id'])
    mqtt.on_connect = on_connect
    mqtt.on_disconnect = on_disconnect
    mqtt.on_message = on_message
    mqtt.username_pw_set(conf['mqtt_user'], conf['mqtt_password'])
    mqtt.enable_logger(logging)
    mqtt.connect(conf['mqtt_server'], conf['mqtt_port'])
    mqtt.loop_start()

    while True:
        loop()

def loop():
    process_pool()
    time.sleep(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Disconnecting...')
        mqtt.disconnect()
        sys.exit(0)
