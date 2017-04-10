#!/usr/bin/env python

import sys
import os
import os.path
import datetime as dt
import time

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollsift import parse

def send_message(topic, info, message_type):
    '''Send message with the given topic and info'''
    pub_ = NoisyPublisher("dummy_sender", 0, topic)
    pub = pub_.start()
    time.sleep(2)
    msg = Message(topic, message_type, info)
    print "Sending message: %s" % str(msg)
    pub.send(str(msg))
    pub_.stop()

def main():
    '''Main.'''

    topic = "/AAPP/1B"

    info_dicts = [{"origin": "157.249.16.170:9060",
                   "uid": "hrpt_metop01_20170307_0638_23180.l1b",
                   "format": "AAPP", "process_time": "2017-03-07T06:41:47",
                   "start_time": "2017-03-07T06:38:00", "compress": ".bz2",
                   "uri": "file://satproc2.met.no/data/trollduction/data/aapp-outdir-ears/metop01_20170307_0638_23180/hrpt_metop01_20170307_0638_23180.l1b",
                   "filename": "hrpt_metop01_20170307_0638_23180.l1b",
                   "platform_name": "Metop-B", "station": "oslo",
                   "end_time": "2017-03-07T06:55:00", "env": "ears", "type": "Binary",
                   "pass_key": "af242ed372824ae51dc4b338e6bdbd02",
                   "sensor": "avhrr/3",
                   "collection_area_id": "eurol", "orbit_number": 23180, "data_processing_level": "1B"},]    

    message_type = 'file'

    topic = "/GLOBAL/METOP/AMV/2d"
    info_dicts = [{"origin": "157.249.16.170:9062", "collection_area_id": "amv_collection", "orbit_number": "23610", "collection": [{"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112203_23609_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:22:03", "end_time": "2017-04-06T11:25:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112203_23609_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112503_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:25:03", "end_time": "2017-04-06T11:28:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112503_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112803_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:28:03", "end_time": "2017-04-06T11:31:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406112803_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113103_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:31:03", "end_time": "2017-04-06T11:34:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113103_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113403_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:34:03", "end_time": "2017-04-06T11:37:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113403_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113703_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:37:03", "end_time": "2017-04-06T11:40:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406113703_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406114003_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T11:40:03", "end_time": "2017-04-06T11:43:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406114003_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406123403_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T12:34:03", "end_time": "2017-04-06T12:37:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406123403_23610_eps_o_amv_l2d.bin"}, {"uid": "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406123703_23610_eps_o_amv_l2d.bin", "start_time": "2017-04-06T12:37:03", "end_time": "2017-04-06T12:40:03", "uri": "file:///data/pytroll/global-metop-amv-l2d/W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+AVHRR_C_EUMP_20170406123703_23610_eps_o_amv_l2d.bin"}], "platform_name": "Metop-B", "end_time": "2017-04-06T12:40:03", "sensor": "amv", "start_time": "2017-04-06T11:25:03"},]

    message_type = 'collection'

    for info_dict in info_dicts:
        send_message(topic, info_dict, message_type)

if __name__ == "__main__":
    main()
