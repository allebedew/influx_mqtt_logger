#!/usr/bin/env python

import json
from pprint import pprint
import sys
import paho.mqtt.client as mqtt_client
import coloredlogs, logging
from influxdb import InfluxDBClient
import time
import argparse

topics = {}     # topic: details

payloads = {}   # topic: payload
last_write = {} # topic: last_write

mqtt = None     # mqtt client
influx = None   # influx client

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
        if topics[topic]['momentary'] == True or topic not in last_write or time.time() - last_write[topic] > args.min_interval:
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
    global topics
    logging.info('Loading config {}...'.format(args.c))

    with open(args.c) as conf_file:
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

def parse_arguments():
    global args
    parser = argparse.ArgumentParser(description='Description string')
    parser.add_argument('-c', metavar='FILE', required=True, help='Comfig file')
    parser.add_argument('--mhost', default='localhost', metavar='HOST', help='MQTT host')
    parser.add_argument('--mport', default='1883', type=int, metavar='PORT', help='MQTT port')
    parser.add_argument('--muser', metavar='USER', help='MQTT user')
    parser.add_argument('--mpass', metavar='PASS', help='MQTT password')
    parser.add_argument('--mclient', default='influx-logger', metavar='CLIENT_ID', help='MQTT clint id')
    parser.add_argument('--ihost', default='localhost', metavar='HOST', help='Influx host')
    parser.add_argument('--iport', default='8086', type=int, metavar='PORT', help='Influx port')
    parser.add_argument('--idb', default='mqtt', metavar='DB', help='Influx database')
    parser.add_argument('--min-interval', default='60', type=int, metavar='DB', help='Min write interval to Influx')
    parser.add_argument('--debug', default=False, type=bool, help='Enable debug logging')
    args = parser.parse_args()
    print(args)

def main():
    global mqtt, influx
    parse_arguments()
    if args.debug:
        coloredlogs.install(level=logging.DEBUG, fmt='%(asctime)s %(levelname)s %(message)s')
    else:
        coloredlogs.install(level=logging.INFO, fmt='%(asctime)s %(levelname)s %(message)s')
    
    load_config()
    # pprint(topics)
    # pprint(conf)

    logging.info('Influx connecting to {}:{} with {} database...'.format(args.ihost, args.iport, args.idb))
    influx = InfluxDBClient(args.ihost, args.iport, 'root', 'root', args.idb)
    influx.create_database(args.idb)

    logging.info('MQTT connecting to {}:{}...'.format(args.mhost, args.mport))
    mqtt = mqtt_client.Client(args.mclient)
    mqtt.on_connect = on_connect
    mqtt.on_disconnect = on_disconnect
    mqtt.on_message = on_message
    if 'muser' in args and 'mport' in args:
        mqtt.username_pw_set(args.muser, args.mpass)
    mqtt.enable_logger(logging)
    mqtt.connect(args.mhost, args.mport)
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
