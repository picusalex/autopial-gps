#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import time

import pynmea2
import serial
import sys

from autopial_lib.thread_worker import AutopialWorker
from autopial_lib.config_driver import ConfigFile
from autopial_lib.utils import safe_float

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
steam_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(asctime)s|%(levelname)08s | %(message)s')
steam_handler.setFormatter(stream_formatter)
logger.addHandler(steam_handler)

class GPSWorker(AutopialWorker):
    def __init__(self, tty, mqtt_client, time_sleep):
        AutopialWorker.__init__(self, mqtt_client, time_sleep, logger=logger)
        logger.info("GPSWorker init with {}".format(tty))
        self._tty = tty
        self.gps_conn = None
        self.gps_connect()

    def gps_connect(self, tty=None):
        if self.gps_conn is not None:
            self.gps_disconnect()

        if tty is not None:
            self._tty = tty

        self.logger.info("Connecting to GPS on '{}'".format(self._tty))
        try:
            self.gps_conn = serial.Serial(
                self._tty,
                115200,
                timeout=0,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
            )
        except serial.serialutil.SerialException as e:
            self.logger.error("Error opening '{}' ({})".format(self._tty, e))
            sys.exit(1)
        self.logger.info("  => successful !")

    def gps_disconnect(self):
        self.gps_conn.close()
        self.gps_conn = None
        self.logger.info("GPS disconnected")

    def run(self):
        logger.info("GPSWorker thread starts")
        if not self.gps_conn.is_open:
            self.gps_connect()

        num_sats = 0
        while self.next():
            try:
                raw_data = self.gps_conn.readline().decode('UTF-8').strip()
            except serial.serialutil.SerialException as e:
                self.logger.error("Error reading on '{}' ({})".format(self._tty, e))
                break

            try:
                nmea_msg = pynmea2.parse(raw_data)
                logger.debug(nmea_msg)
            except pynmea2.nmea.ParseError as e:
                #logger.debug("Invalid NMEA sentence: ".format(e))
                time.sleep(0.1)
                continue

            if isinstance(nmea_msg, pynmea2.GGA):
                num_sats = nmea_msg.num_sats
                topic = "autopial/gps/location"
                value = {
                    "fix": nmea_msg.is_valid,
                    "latitude": round(nmea_msg.latitude,6),
                    "longitude": round(nmea_msg.longitude,6),
                    "altitude": nmea_msg.altitude,
                }
                self.publish(topic, value)

            if isinstance(nmea_msg, pynmea2.RMC):
                topic = "autopial/gps/movement"
                value = {
                    "fix": nmea_msg.is_valid,
                    "speed": nmea_msg.spd_over_grnd,
                    "direction": nmea_msg.true_course,
                }
                self.publish(topic, value)

            if isinstance(nmea_msg, pynmea2.GSA):
                topic = "autopial/gps/infos"
                value = {
                    "fix": nmea_msg.is_valid,
                    "precision_horiz": safe_float(nmea_msg.hdop),
                    "precision_vert": safe_float(nmea_msg.vdop),
                    "satellites": num_sats
                }
                self.publish(topic, value)

            time.sleep(0.25)

        self.gps_disconnect()
        logger.info("GPSWorker thread ends")


if __name__ == '__main__':
    cfg = ConfigFile("autopial-gps.cfg", logger=logger)
    try:
        tty = cfg.get("gps_device")
        publish_every = cfg.get("publish_every")
    except BaseException as e:
        logger.error("Invalid config file: {}".format(e))
        sys.exit(1)

    worker_gps = GPSWorker(tty, "GPSWorker", time_sleep=publish_every)
    worker_gps.start()

    try:
        while 1:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        worker_gps.stop()


