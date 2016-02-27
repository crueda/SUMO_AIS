#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-12-21
# mail: carlos.rueda@deimos-space.com
# version: 1.0

##################################################################################
# version 1.0 release notes:
# Initial version
##################################################################################

from __future__ import division
import time
import datetime
import os
import sys
import utm
import SocketServer, socket
import logging, logging.handlers
import json
import httplib2
from threading import Thread
import pika


########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./ais_dispatcher.properties')

LOG = config['directory_logs'] + "/ais_dispatcher.log"
LOG_FOR_ROTATE = 10

RABBITMQ_HOST = config['rabbitMQ_HOST']
RABBITMQ_PORT = config['rabbitMQ_PORT']
RABBITMQ_ADMIN_USERNAME = config['rabbitMQ_admin_username']
RABBITMQ_ADMIN_PASSWORD = config['rabbitMQ_admin_password']
QUEUE_NAME = config['queue_name']

KCS_HOST = config['KCS_HOST']
KCS_PORT = config['KCS_PORT']

DEFAULT_SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/ais_dispatcher"

########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('ais_dispatcher')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if ais_dispatcher process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the ais_dispatcher process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start ais_dispatcher process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "ais_dispatcher process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

########################################################################
# Definicion de funciones
#
########################################################################

socketKCS = None
SLEEP_TIME = DEFAULT_SLEEP_TIME

def connectKCS():
    global socketKCS

    # conexion a KCS
    socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socketKCS.settimeout(0.1)
    try:
        socketKCS.connect((KCS_HOST, int(KCS_PORT)))
        logger.info('Connected to KCS')
        socketKCS.settimeout(0.05)
    except Exception, error:
        logger.error('Error connecting to KCS: %s', error)

def send2kcs(message):
    global socketKCS

    try:
        socketKCS.send(message)
        logger.info("[->] Sent to KCS")
        return True
    except Exception, error:
        logger.error('Error sending data to KCS: %s',error)
        logger.info('Trying reconnection to KCS')
        try:
            socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socketKCS.settimeout(0.1)
            socketKCS.connect((KCS_HOST, int(KCS_PORT)))
            socketKCS.settimeout(0.05)
            socketKCS.send(message)
            logger.info("[->] Sent to KCS")
            return True
        except:
            logger.info('Failed reconnection to KCS')
            return False


def calculateSleep():
    global SLEEP_TIME
    global DEFAULT_SLEEP_TIME
    url = "http://" + RABBITMQ_HOST + ":" + RABBITMQ_PORT + "/api/queues/%2F/" + QUEUE_NAME 
    h = httplib2.Http(".cache")
    h.add_credentials(RABBITMQ_ADMIN_USERNAME, RABBITMQ_ADMIN_PASSWORD) 
    
    # Cada 20 segundos se lee el tamaño de la cola para recalcular el SLEEP_TIME
    while True:
        resp, content = h.request(url)
        if (resp.status == 200):
            content_json = json.loads(content)
            if (content_json['messages'] == 0):
                SLEEP_TIME = DEFAULT_SLEEP_TIME
            else:
                SLEEP_TIME = 15/content_json['messages']
            logger.info ("Mensajes en cola=" + str(content_json['messages']) + " - SLEEP TIME=" + str(SLEEP_TIME))
        time.sleep(20)

def proccessQueue():
    global rabbitMQconnection
    while True:
        try:
            if (rabbitMQconnection == None):
                rabbitMQconnection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

            channel = rabbitMQconnection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            logger.info(' [*] Waiting for messages. To exit press CTRL+C')

            def callback(ch, method, properties, body):
                logger.debug(" [x] Received %r" % body)
        
                # Bucle de envio del mensaje al KCS
                resultKCSK = False
                while resultKCSK != True:
                    resultKCSK = send2kcs(body)
                    time.sleep(0.2)
                
                # Confirma la lectura del mensaje    
                ch.basic_ack(delivery_tag = method.delivery_tag)
                
                # Espera antes de leer el siguiente mensaje
                #print "Espera despues de leer: " + str(SLEEP_TIME)
                time.sleep(SLEEP_TIME)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(callback,
                              queue=QUEUE_NAME)

            channel.start_consuming()

        except Exception, error:
            logger.error('Error connecting to rabbitMQ: %s', error)
            rabbitMQconnection = None
            time.sleep(0.2)

########################################################################
# Funcion principal
#
########################################################################

def main():
    global SLEEP_TIME
    rabbitMQconnection = None

    thread1 = Thread(target=calculateSleep)
    thread1.start()

    thread2 = Thread(target=proccessQueue)
    thread2.start()

    thread1.join()
    thread2.join()

    

if __name__ == '__main__':
    connectKCS()
    main()
