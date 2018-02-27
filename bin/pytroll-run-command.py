#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016

# Author(s):

#   Trygve Aspenes

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Run a configured command for configured topic
"""

import sys
import os
import logging
from logging import handlers
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
from urlparse import urlparse
from trollsift.parser import compose
from datetime import datetime

from subprocess import Popen, PIPE
import threading
import Queue

# ----------------------------
# Default settings for logging
# ----------------------------
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

class FilePublisher(threading.Thread):

    """A publisher for result files. Picks up the return value from the
    run_command when ready, and publishes the files via posttroll"""

    def __init__(self, queue, config, command_name):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.config = config
        self.command_name = command_name

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        try:
            service_name = 'run_command_' + self.command_name
            LOG.debug("Using service_name: {}".format(service_name))
            with Publish(service_name, 0, [self.config['publish-topic'], ], nameservers=self.config['nameservers']) as publisher:

                while self.loop:
                    retv = self.queue.get()

                    if retv != None:
                        LOG.info("Publish the files...")
                        publisher.send(retv)

        except KeyboardInterrupt as ki:
            LOG.info("Received keyboard interrupt. Shutting down")
        finally:
            LOG.info("Exiting publisher in pps_runner. See ya")

class FileListener(threading.Thread):

    def __init__(self, queue, config):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):
        if type(self.config["subscribe-topic"]) not in (tuple, list, set):
            self.config["subscribe-topic"] = [self.config["subscribe-topic"]]
        try:
            with posttroll.subscriber.Subscribe(self.config['services'], self.config['subscribe-topic'],
                                                True) as subscr:

                for msg in subscr.recv(timeout=90):
                    if not self.loop:
                        break

                    # Check if it is a relevant message:
                    if self.check_message(msg):
                        LOG.info("Put the message on the queue...")
                        LOG.debug("Message = " + str(msg))
                        self.queue.put(msg)
        except KeyboardInterrupt as ki:
            LOG.info("Received keyboard interrupt. Shutting down")
        finally:
            LOG.info("Exiting subscriber in pps_runner. See ya")

    def check_message(self, msg):

        if not msg:
            return False

        if 'providing-server' in self.config:
            if msg.host not in self.config['providing-server']:
                LOG.debug("Not the providing server. Providing must be: {} while message is from {}.".format(self.config['providing-server'],msg.host))
                LOG.debug("Skip this.");
                return False
                
        if 'sensor' in self.config:
            if 'sensor' in msg.data:
                LOG.debug("Check sensor.")
                if self.config['sensor'] in msg.data['sensor']:
                    LOG.debug("Sensor match.")
                else:
                    LOG.debug("Not Sensor match. Skip this.")
                    LOG.debug("config: {}, message: {}".format(self.config['sensor'],msg.data['sensor']))
                    return False
            else:
                LOG.debug("Sensor not in message. Skip this.")
                return False
        else:
            LOG.debug("Sensor not in config. Skip this.")
            return False
        
        if 'collection_area_id' in self.config:
            if 'collection_area_id' in msg.data:
                LOG.debug("Check collection area id.")
                if self.config['collection_area_id'] in msg.data['collection_area_id']:
                    LOG.debug("collection area id match: {}".format(self.config['collection_area_id']))
                else:
                    LOG.debug("No collection area id match. Skip this.")
                    LOG.debug("config: {}, message: {}".format(self.config['collection_area_id'],msg.data['collection_area_id']))
                    return False
            else:
                LOG.debug("collection_area_id not in message. Skip this.")
                return False
        else:
            LOG.debug("collection_area_id not in config. Process anyway.")
                
        if 'uri' in msg.data:
            msg.data['uri'] = urlparse(msg.data['uri']).path
        elif msg.type == 'dataset':
            if 'dataset' in msg.data:
                for i, col in enumerate(msg.data['dataset']):
                    if 'uri' in col:
                        urlobj = urlparse(col['uri'])
                        msg.data['dataset'][i]['uri'] = urlobj.path
                        if 'file_list' in msg.data:
                            msg.data['file_list'] += " "
                            msg.data['file_list'] += urlobj.path
                        else:
                            msg.data['file_list'] = urlobj.path

                        if 'path' in msg.data and msg.data['path']:
                            if msg.data['path'] != os.path.dirname(urlobj.path):
                                LOG.error("Path differs from previous path. This will cause problems.")
                                LOG.warning("previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOG.debug("Path is {}".format(msg.data['path']))
                    else:
                        LOG.error("URI not found in dataset")
        elif msg.type == 'collection':
            if 'collection' in msg.data:
                for i, col in enumerate(msg.data['collection']):
                    if 'uri' in col:
                        urlobj = urlparse(col['uri'])
                        msg.data['collection'][i]['uri'] = urlobj.path
                        if 'file_list' in msg.data:
                            msg.data['file_list'] += " "
                            msg.data['file_list'] += urlobj.path
                        else:
                            msg.data['file_list'] = urlobj.path

                        if 'path' in msg.data and msg.data['path']:
                            if msg.data['path'] != os.path.dirname(urlobj.path):
                                LOG.error("Path differs from previous path. This will cause problems.")
                                LOG.warning("previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOG.debug("Path is {}".format(msg.data['path']))
                    elif 'dataset' in col:
                        for key_i, val_col in enumerate(col['dataset']):
                            if 'uri' in val_col:
                                urlobj = urlparse(val_col['uri'])
                                if 'file_list' in msg.data:
                                    msg.data['file_list'] += " "
                                    msg.data['file_list'] += urlobj.path
                                else:
                                    msg.data['file_list'] = urlobj.path

                                if 'path' in msg.data:
                                    if msg.data['path'] != os.path.dirname(urlobj.path):
                                        LOG.warning("Path differs from previous path. This can cause problems if 'path' keyword is used.")
                                        LOG.warning("Keeping previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
                                else:
                                    msg.data['path'] = os.path.dirname(urlobj.path)
                                
                    else:
                        LOG.warning("No uri or dataset in collection")
        else:
            LOG.debug("uri not in message. Skip this.")
            return False
        
        if 'resolution' in self.config:
            if 'resolution' in msg.data:
                if self.config['resolution'] == msg.data['resolution']:
                    LOG.debug("process this resolution")
                else:
                    LOG.debug("Resolution config and message don't match up: {} vs {}".format(self.config['resolution'],msg.data['resolution']))
                    LOG.debug("Skip this")
                    return False
            
        return True

def read_arguments():
    """
    Read command line arguments
    Return
    command name, config file and log file
    """
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='',
                        help="The file containing " +
                        "configuration parameters e.g. pytroll-run-command.cfg")
    parser.add_argument("-n", "--command_name",
                        help="Name of the command",
                        dest="command_name",
                        type=str,
                        default="unknown")
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")
    parser.add_argument("-l", "--log", help="File to log to",
                        type=str,
                        default=None)

    args = parser.parse_args()

    if args.config_file == '':
        print "Configuration file required! pytroll-run-command.py -c <config-file>"
        sys.exit()
    if args.command_name == '':
        print "Command name required! Use command-line switch -n <command-name>"
        sys.exit()
    else:
        command_name = args.command_name.lower()

    if 'template' in args.config_file:
        print "Template file given as master config, aborting!"
        sys.exit()

    return command_name, args.config_file, args.log


def read_config_file_options(filename, command_name, valid_config=None):
    """
    Read and checks config file
    If ok, return configuration dictionary
    """

    import yaml
    print "About to read yaml config ... "
    print "And using command_name: {}".format(command_name)
    with open(filename, 'r') as stream:
        try:
            config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    if 'services' not in config[command_name]:
        config[command_name]['services'] = ""
        
    if 'nameservers' not in config[command_name]:
        config[command_name]['nameservers'] = None

    if 'force_processing_of_repeating_messages' not in config[command_name]:
        config[command_name]['force_processing_of_repeating_messages'] = None

    return config

def setup_logging(config, log_file):
    """
    Init and setup logging
    """

    if log_file is not None:
        try:
            ndays = int(config['logging']["log_rotation_days"])
            ncount = int(config['logging']["log_rotation_backup"])
        except KeyError as err:
            print err.args, \
                "is missing. Please, check your config ",\
                config
            #FIXME Make the errorhandeling better
            raise IOError("Config was given but doesn't " +
                          "know how to backup and rotate")

        handler = handlers.TimedRotatingFileHandler(log_file,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    if (config['logging']["logging_mode"] and
            config['logging']["logging_mode"] == "DEBUG"):
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)

    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('pytroll-run-command')
    
    return LOG

def logreader(stream, log_func, output):
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
        output.append(s)
    stream.close()

def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Register didn't contain any entry matching: " +
                    str(key))

    return

def terminate_process(popen_obj, scene):
    """Terminate a Popen process"""
    if popen_obj.returncode == None:
        popen_obj.kill()
        LOG.info("Process timed out and pre-maturely terminated. Scene: " + str(scene))
    else:
        LOG.info("Process finished before time out - workerScene: " + str(scene))
    return

def get_outputfiles_from_stdout(stdout, config):

    import re
    result_files = {}
    default_match = ["Start\scompressing\sand\swriting\s(.*)\s\.\.\.",]
    if 'stdout-match' in config:
        match_list = config['stdout-match']
    else:
        match_list = default_match

    for line in stdout:
        for mtch in match_list:
            match = re.search(mtch, line)
            if match:
                LOG.debug("Matching filename: {}".format(match.group(1)))
                if match.group(1) in result_files:
                    result_files[match.group(1)]+=1
                else:
                    result_files[match.group(1)] = 1
            
    return result_files

def command_handler(semaphore_obj, config, job_dict, job_key, publish_q, input_msg):

    try:
        LOG.debug("Waiting for acquired semaphore...")
        with semaphore_obj:
            LOG.debug("Acquired semaphore")
            stdout = []
            stderr = []
            threads__ = []
            #out_readers = []
            #err_readers = []

            for command in config['command']:
                try:
                    cmd = compose(command,input_msg.data)
                    import shlex
                    myargs = shlex.split(str(cmd))
                    LOG.debug('Command sequence= ' + str(myargs))
                    my_env = None
                    my_cwd = None
                    if 'environment' in config:
                        my_env = config['environment']
                    if 'working_directory' in config:
                        my_cwd = config['working_directory']
                    if 'working_directory_mkdtemp' in config:
                        my_cwd = config['working_directory_mkdtemp']
                        import tempfile
                        LOG.debug("About to make temp dir in : {}".format(my_cwd))
                        my_cwd=tempfile.mkdtemp(dir=my_cwd)
                        LOG.debug("working_directory_mkdtemp: my_cwd: {}".format(my_cwd))
                    cmd_proc = Popen(myargs, env=my_env, shell=False, stderr=PIPE, stdout=PIPE, cwd=my_cwd)
                except:
                    LOG.exception("Failed in command... {}".format(sys.exc_info()))

                t__ = threading.Timer(20 * 60.0, terminate_process, args=(cmd_proc, config, ))
                threads__.append(t__)
                t__.start()

                out_reader = threading.Thread(target=logreader, args=(cmd_proc.stdout, LOG.info, stdout))
                err_reader = threading.Thread(target=logreader, args=(cmd_proc.stderr, LOG.info, stderr))
                #out_readers.append(out_reader)
                #err_readers.append(err_reader)
                out_reader.start()
                err_reader.start()

                out_reader.join()
                err_reader.join()
                LOG.info("Ready with command run.")

                if 'working_directory_mkdtemp' in config:
                    import shutil
                    LOG.debug("About to remove temp dir: {}".format(my_cwd))                    
                    shutil.rmtree(my_cwd)
                    LOG.debug("removed: {}".format(my_cwd))                    
            #for out_reader__ in out_readers:
            #    out_reader__.join()
            #for err_reader__ in err_readers:
            #    err_reader__.join()


            result_files = get_outputfiles_from_stdout(stdout, config)
            
            if 'publish-all-files-as-collection' in config and config['publish-all-files-as-collection']:
                LOG.debug("publish all file as collection")
                files = []
                for result_file,number in result_files.iteritems():
                    file_list = {}
                    if not os.path.exists(result_file):
                        LOG.error("File {} does not exists after production. Do not publish.".format(result_file))
                        continue
                    file_list['uri'] = result_file
                    filename = os.path.split(result_file)[1]
                    LOG.info("file to publish = " + str(filename))
                    file_list['uid'] = filename
                    files.append(file_list)

                if files:
                    to_send = input_msg.data.copy()
                    to_send.pop('dataset', None)
                    to_send.pop('collection', None)
                    to_send.pop('filename',None)
                    to_send.pop('compress',None)
                    to_send.pop('tst',None)
                    to_send.pop('uri',None)
                    to_send.pop('uid',None)
                    to_send['collection'] = files

                    pubmsg = Message(config['publish-topic'], "collection", to_send).encode()
                    LOG.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
                else:
                    LOG.warning("Found no files after run command. No files to publish.")

            else:
                # Now publish:
                for result_file,number in result_files.iteritems():
                    if not os.path.exists(result_file):
                        LOG.error("File {} does not exits after production. Do not publish.".format(result_file))
                        continue

                    filename = os.path.split(result_file)[1]
                    LOG.info("file to publish = " + str(filename))

                    to_send = input_msg.data.copy()
                    to_send.pop('dataset', None)
                    to_send.pop('collection', None)
                    to_send.pop('filename',None)
                    to_send.pop('compress',None)
                    to_send.pop('tst',None)
                    to_send.pop('file_list',None)
                    to_send.pop('path',None)

                    to_send['uri'] = result_file
                    to_send['uid'] = filename
                    if result_file.endswith("xml"):
                        to_send['format'] = 'PPS-XML'
                        to_send['type'] = 'XML'
                    if result_file.endswith("nc"):
                        to_send['format'] = 'CF'
                        to_send['type'] = 'netCDF4'
                    if result_file.endswith("h5"):
                        to_send['format'] = 'PPS'
                        to_send['type'] = 'HDF5'
                    if result_file.endswith("mitiff"):
                        to_send['format'] = 'MITIFF'
                        to_send['type'] = 'MITIFF'

                    to_send['data_processing_level'] = '2'

                    pubmsg = Message(config['publish-topic'], "file", to_send).encode()
                    LOG.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
                else:
                    LOG.info("No matching files to publish")

            for thread__ in threads__:
                thread__.cancel()

    except:
        LOG.exception('Failed in command_handler...')
        raise

def ready2run(msg, job_register, sceneid):
    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOG.debug("Processing of scene " + str(sceneid) +
                  " have already been launched...")
        return False

    job_register[sceneid] = datetime.utcnow()

    return True

if __name__ == "__main__":

    """
    Call the various functions that make up the parts of the AAPP processing
    """
    
    #Read the command line argument
    (command_name, config_filename, log_file) = read_arguments()

    if not os.path.isfile(config_filename):
        print "ERROR! Can not find config file: {}".format(config_filename)
        print "Exits!"
        sys.exit()

    config = read_config_file_options(config_filename, command_name)

    #Set up logging
    try:
        LOG = setup_logging(config, log_file)
    except:
        print "Logging setup failed. Check your config"
        #TODO
        #Better error handeling for logging setup

    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    LOG.debug("\n{}".format(pp.pformat(config[command_name])))

    try:
        sema = threading.Semaphore(5)
        listener_q = Queue.Queue()
        publisher_q = Queue.Queue()
        
        pub_thread = FilePublisher(publisher_q, config[command_name], command_name)
        pub_thread.start()
        listen_thread = FileListener(listener_q, config[command_name])
        listen_thread.start()

        threads = []
        jobs_dict = {}
        while True:

            try:
                msg = listener_q.get()
            except Queue.Empty:
                continue

            LOG.debug("Number of threads currently alive: " + str(threading.active_count()))

            if 'orbit_number' not in msg.data:
                msg.data['orbit_number'] = '00000'

            if 'start_time' not in msg.data:
                if 'nominal_time' in msg.data:
                    msg.data['start_time'] = msg.data['nominal_time']
                else:
                    LOG.error("Can not find a time to use for start_time.")

            keyname = (str(msg.data['platform_name']) + '_' +
                       str(msg.data['orbit_number']) + '_' +
                       str(msg.data['start_time'].strftime('%Y%m%d%H%M')))

            if config[command_name]['force_processing_of_repeating_messages']:
                LOG.debug("Force processing even if run before.")
                jobs_dict[keyname] = datetime.utcnow()
            elif not ready2run(msg, jobs_dict, keyname):
                continue

            if keyname not in jobs_dict:
                LOG.warning("Scene-run seems unregistered! Forget it...")
                continue

            t__ = threading.Thread(target=command_handler, args=(sema, config[command_name],
                                                            jobs_dict,
                                                            keyname,
                                                            publisher_q,
                                                            msg))
            threads.append(t__)
            t__.start()

            LOG.debug("Number of threads currently alive: " + str(threading.active_count()))

            # Block any future run on this scene for x minutes from now
            # x = 20
            thread_job_registry = threading.Timer(20 * 60.0, reset_job_registry, args=(jobs_dict, keyname))
            thread_job_registry.start()

        LOG.info("Wait till all threads are dead...")
        while True:
            workers_ready = True
            for thread in threads:
                if thread.is_alive():
                    workers_ready = False
                    
                    if workers_ready:
                        break
                    
        pub_thread.stop()
        listen_thread.stop()

    except:
        LOG.error("Unexpected error in thread/semaphore/listen/publish loop {}".format(sys.exc_info()))
