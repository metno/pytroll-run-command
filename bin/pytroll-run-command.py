#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016-2020

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

import os
import sys
import six
import time
import json
import logging
from logging import handlers
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
from trollsift.parser import compose
from datetime import datetime

from subprocess import Popen, PIPE
import threading
try:
    import queue
except ImportError:
    import Queue as queue
import pyinotify
import yaml
import argparse
import signal
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

running = True
chains = {}

# ----------------------------
# Default settings for logging
# ----------------------------
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


# Config management
def read_config(filename, debug=True):
    """Read the config file called *filename*.
    """
    with open(filename, 'r') as stream:
        try:
            config = yaml.load(stream)
            if debug:
                import pprint
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(config)
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    return config


def terminate(chains):
    try:
        for chain in chains.itervalues():
            LOGGER.debug("Terminate on chain: {}".format(chain))
            #chain_listeners = chain["listeners"]
            #if not isinstance(chain_listeners, list):
            #    chain_listeners = list(chain_listeners)
            #for listener in chain_listeners:
            LOGGER.debug("stop on listener: {}".format(chain["listeners"]))
            chain['listeners'].stop()
            del chain['listeners']
    except AttributeError:
        for chain in chains:
            LOGGER.debug("Terminate on chain: {}".format(chain))
            LOGGER.debug("stop on listener: {}".format(chains[chain]["listeners"]))
            chains[chain]['listeners'].stop()
            del chains[chain]['listeners']

    LOGGER.info("Shutting down.")
    print("Thank you for using run-command.")
    time.sleep(1)
    sys.exit(0)


def read_from_queue(queue, service_name_publisher):
    # read from queue
    threads = []
    thread_job_registry_list = []
    while True:
        LOGGER.debug("Check threads list if is alive ... lenght of threads list %d", len(threads))
        for thr in threads[:]:
            if thr.is_alive():
                LOGGER.debug("Thread is alive: %s", str(thr.ident))
            else:
                LOGGER.debug("Thread is not alive %s", str(thr))
                LOGGER.debug("Try to join ... ")
                thr.join()
                threads.remove(thr)
        LOGGER.debug("Check thread job registry list if is alive ... lenght of threads list %d", len(thread_job_registry_list))
        for thr in thread_job_registry_list[:]:
            if thr.is_alive():
                LOGGER.debug("Thread job registry is alive: %s", str(thr.ident))
            else:
                LOGGER.debug("Thread job registry is not alive %s", str(thr))
                LOGGER.debug("Try to join ... ")
                thr.join()
                thread_job_registry_list.remove(thr)
        LOGGER.debug("Start reading from queue ... ")
        msg_data = queue.get()
        if msg_data is None:
            LOGGER.debug("msg is none ... ")
            continue
        elif msg_data == 'kill':
            LOGGER.debug("Killing read from queue loop")
            for thr in threads[:]:
                thr.join()
            for thr in thread_job_registry_list[:]:
                thr.join()
            break
        LOGGER.debug("Read from queue ... ")
        msg = msg_data['msg']
        config = msg_data['config']
        command_name = msg_data['command_name']
        LOGGER.debug("Read from queue: {}".format(msg))
        LOGGER.debug("Read from queue: Number of threads currently alive: " + str(threading.active_count()))

        if 'orbit_number' not in msg.data:
            msg.data['orbit_number'] = '00000'

        if 'start_time' not in msg.data:
            if 'nominal_time' in msg.data:
                msg.data['start_time'] = msg.data['nominal_time']
            else:
                LOGGER.error("Can not find a time to use for start_time.")

        keyname = (str(command_name) + '_' +
                   str(msg.data['platform_name']) + '_' +
                   str(msg.data['orbit_number']) + '_' +
                   str(msg.data['start_time'].strftime('%Y%m%d%H%M')))

        if 'force_processing_of_repeating_messages' in config and config['force_processing_of_repeating_messages']:
            LOGGER.debug("Force processing even if run before.")
            jobs_dict[keyname] = datetime.utcnow()
        elif not ready2run(msg, jobs_dict, keyname):
            continue

        if keyname not in jobs_dict:
            LOGGER.warning("Scene-run seems unregistered! Forget it...")
            continue

        t__ = threading.Thread(target=command_handler, args=(sema, config,
                                                             jobs_dict,
                                                             keyname,
                                                             publisher_q,
                                                             msg,
                                                             command_name,
                                                             service_name_publisher))
        # TODO Is this needed?
        threads.append(t__)
        t__.start()
        # LOGGER.debug("command handler thread object: {}".format(t__))
        LOGGER.debug("Number of threads currently alive: " + str(threading.active_count()))
        # LOGGER.debug("Thread objects alive: " + str(threading.enumerate()))

        # Block any future run on this scene for x minutes from now
        # x = 20
        # Set this to 20 seconds to avoid several hundred waiting threads
        try:
            thread_job_registry = threading.Timer(20, reset_job_registry, args=(jobs_dict, keyname))
            thread_job_registry_list.append(thread_job_registry)
            thread_job_registry.start()
        except Exception as e:
            LOGGER.exception("Exception in starting new thread: %s", str(e))
            raise
        LOGGER.debug("Number of thread job regestry entries currently alive: " + str(threading.active_count()))

        # If block option is given, wait for the job to complete before it continues.
        if 'block_run_until_complete' in config and config['block_run_until_complete']:
            LOGGER.debug("Waiting until the run is complete before continuing ...")
            t__.join()
            LOGGER.debug("Run complete!")


# Event handler. Needed to handle reload of config
class EventHandler(pyinotify.ProcessEvent):
    """Handle events with a generic *fun* function.
    """

    def __init__(self, fun, *args, **kwargs):
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._fun = fun

    def process_IN_CLOSE_WRITE(self, event):
        """On closing after writing.
        """
        self._fun(event.pathname)

    def process_IN_CREATE(self, event):
        """On closing after linking.
        """
        try:
            if os.stat(event.pathname).st_nlink > 1:
                self._fun(event.pathname)
        except OSError:
            return

    def process_IN_MOVED_TO(self, event):
        """On closing after moving.
        """
        self._fun(event.pathname)


class FilePublisher(threading.Thread):

    """A publisher for result files. Picks up the return value from the
    run_command when ready, and publishes the files via posttroll"""

    def __init__(self, queue, nameservers, service_name):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.service_name = service_name
        self.nameservers = nameservers

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        try:
            self.loop = True
            LOGGER.debug("Using service_name: {} with nameservers {}".format(self.service_name, self.nameservers))
            with Publish(self.service_name, 0, nameservers=self.nameservers) as publisher:

                while self.loop:
                    retv = self.queue.get()

                    if retv is not None:
                        LOGGER.info("Publish as service: %s", self.service_name)
                        LOGGER.info("Publish the files...")
                        publisher.send(retv)

        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Shutting down")
        # finally:
        #    LOGGER.info("Exiting publisher in run-command. See ya")


class FileListener(threading.Thread):

    def __init__(self, queue, config, command_name):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config
        self.subscr = None
        self.command_name = command_name

    def stop(self):
        """Stops the file listener"""
        LOGGER.debug("Entering stop in FileListener ...")
        self.loop = False
        self.queue.put(None)

    def run(self):
        LOGGER.debug("Entering run in FileListener ...")
        if type(self.config["subscribe-topic"]) not in (tuple, list, set):
            self.config["subscribe-topic"] = [self.config["subscribe-topic"]]
        try:
            if 'services' not in self.config:
                self.config['services'] = ''
            with posttroll.subscriber.Subscribe(self.config['services'], self.config['subscribe-topic'],
                                                True) as subscr:

                LOGGER.debug("Entering for loop subscr.recv")
                for msg in subscr.recv(timeout=1):
                    if not self.loop:
                        # LOGGER.debug("Self.loop false in FileListener {}".format(self.loop))
                        break

                    # Check if it is a relevant message:
                    if self.check_message(msg):
                        LOGGER.info("Put the message on the queue...")
                        LOGGER.debug("Message = " + str(msg))
                        msg_data = {}
                        msg_data['config'] = self.config
                        msg_data['msg'] = msg
                        msg_data['command_name'] = self.command_name
                        self.queue.put(msg_data)
                        LOGGER.debug("After queue put.")

        except KeyError as ke:
            LOGGER.info("Some key error. probably in config:", ke)
            raise
        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Shutting down.")
            raise
        # finally:
        #    LOGGER.info("Exiting subscriber in run-command. See ya")

    def check_message(self, msg):

        if not msg:
            return False

        if 'providing-server' in self.config:
            if msg.host not in self.config['providing-server']:
                LOGGER.debug("Not the providing server. Providing must be: {} while message is from {}.".format(
                    self.config['providing-server'], msg.host))
                LOGGER.debug("Skip this.")
                return False

        if 'sensor' in self.config:
            if type(self.config["sensor"]) not in (list,):
                LOGGER.debug("Convert sensor config to list")
                self.config["sensor"] = [self.config["sensor"]]
            if 'sensor' in msg.data:
                if type(msg.data["sensor"]) not in (list,):
                    LOGGER.debug("Convert sensor message to list")
                    msg.data["sensor"] = [msg.data["sensor"]]
                LOGGER.debug("Check sensor.")
                if any([sensor in self.config['sensor'] for sensor in msg.data['sensor']]):
                    LOGGER.debug("Sensor match from message %s is in config %s", str(msg.data['sensor']), str(self.config['sensor']))
                else:
                    LOGGER.debug("Not Sensor match. Skip this.")
                    LOGGER.debug("config: {}, message: {}".format(self.config['sensor'], msg.data['sensor']))
                    return False
            else:
                LOGGER.debug("Sensor not in message. Skip this.")
                return False
        else:
            if 'sensor' in msg.data:
                LOGGER.debug("Sensor not in config, but in message. Skip this.")
                return False
            else:
                LOGGER.debug("Sensor not in config, nor in message. Process anyway.")

        if 'collection_area_id' in self.config:
            if 'collection_area_id' in msg.data:
                LOGGER.debug("Check collection area id.")
                if self.config['collection_area_id'] in msg.data['collection_area_id']:
                    LOGGER.debug("collection area id match: {}".format(self.config['collection_area_id']))
                else:
                    LOGGER.debug("No collection area id match. Skip this.")
                    LOGGER.debug("config: {}, message: {}".format(self.config['collection_area_id'],
                                                                  msg.data['collection_area_id']))
                    return False
            else:
                LOGGER.debug("collection_area_id not in message. Skip this.")
                return False
        else:
            LOGGER.debug("collection_area_id not in config. Process anyway.")
        LOGGER.debug("msg.data %s", str(msg.data))

        # Added the msg type check is file. Must check if this works
        LOGGER.debug("msg.type %s", str(msg.type))
        if msg.type == 'file' and 'uri' in msg.data:
            LOGGER.debug("uri in msg.data")
            msg.data['uri'] = urlparse(msg.data['uri']).path
            msg.data['path'] = os.path.dirname(msg.data['uri'])
        elif msg.type == 'dataset':
            LOGGER.debug(" msg.type is dataset")
            if 'dataset' in msg.data:
                LOGGER.debug("dataset in msg.data")
                for i, col in enumerate(msg.data['dataset']):
                    if 'uri' in col:
                        urlobj = urlparse(col['uri'])
                        if col['uri'].startswith('//', 0, 2):
                            urlobj = urlparse(col['uri'].replace('//', '/'))

                        msg.data['dataset'][i]['uri'] = urlobj.path
                        if 'file_list' in msg.data:
                            msg.data['file_list'] += " "
                            msg.data['file_list'] += urlobj.path
                        else:
                            msg.data['file_list'] = urlobj.path

                        if 'path' in msg.data and msg.data['path']:
                            if msg.data['path'] != os.path.dirname(urlobj.path):
                                LOGGER.error("Path differs from previous path. This will cause problems.")
                                LOGGER.warning("previous path: {}, this path is : {}".format(
                                    msg.data['path'],
                                    os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOGGER.debug("Path is {}".format(msg.data['path']))
                    else:
                        LOGGER.error("URI not found in dataset")
        elif msg.type == 'collection':
            LOGGER.debug("msg.type is collection")
            if 'collection' in msg.data:
                LOGGER.debug("collection in msg.data")
                for i, col in enumerate(msg.data['collection']):
                    if 'uri' in col:
                        urlobj = urlparse(col['uri'])
                        if col['uri'].startswith('//', 0, 2):
                           urlobj = urlparse(col['uri'].replace('//', '/'))

                        msg.data['collection'][i]['uri'] = urlobj.path
                        if 'file_list' in msg.data:
                            msg.data['file_list'] += " "
                            msg.data['file_list'] += urlobj.path
                        else:
                            msg.data['file_list'] = urlobj.path

                        if 'path' in msg.data and msg.data['path']:
                            if msg.data['path'] != os.path.dirname(urlobj.path):
                                LOGGER.error("Path differs from previous path. This will cause problems.")
                                LOGGER.warning("previous path: {}, this path is : {}".format(
                                    msg.data['path'], os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOGGER.debug("Path is {}".format(msg.data['path']))
                    elif 'dataset' in col:
                        for key_i, val_col in enumerate(col['dataset']):
                            if 'uri' in val_col:
                                urlobj = urlparse(val_col['uri'])
                                if val_col['uri'].startswith('//', 0, 2):
                                    urlobj = urlparse(val_col['uri'].replace('//', '/'))

                                if 'file_list' in msg.data:
                                    msg.data['file_list'] += " "
                                    msg.data['file_list'] += urlobj.path
                                else:
                                    msg.data['file_list'] = urlobj.path

                                if 'path' in msg.data:
                                    if msg.data['path'] != os.path.dirname(urlobj.path):
                                        LOGGER.warning("Path differs from previous path. This can cause problems if 'path' keyword is used.")
                                        LOGGER.warning("Keeping previous path: {}, this path is : {}".format(
                                            msg.data['path'], os.path.dirname(urlobj.path)))
                                else:
                                    msg.data['path'] = os.path.dirname(urlobj.path)

                    else:
                        LOGGER.warning("No uri or dataset in collection")
        else:
            LOGGER.debug("uri not in message. Skip this.")
            return False

        if 'resolution' in self.config:
            if 'resolution' in msg.data:
                if self.config['resolution'] == msg.data['resolution']:
                    LOGGER.debug("process this resolution")
                else:
                    LOGGER.debug("Resolution config and message don't match up: {} vs {}".format(
                        self.config['resolution'], msg.data['resolution']))
                    LOGGER.debug("Skip this")
                    return False

        return True


def setup_logging(config_file, log_file):
    """
    Init and setup logging
    """
    config = None
    if os.path.exists(config_file):
        config = read_config(config_file, debug=False)

    loglevel = logging.INFO
    if log_file and 'logging' in config:
        ndays = 1
        ncount = 30
        try:
            ndays = int(config['logging']["log_rotation_days"])
            ncount = int(config['logging']["log_rotation_backup"])
        except KeyError:
            pass

        handler = handlers.TimedRotatingFileHandler(os.path.join(log_file),
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    if 'logging_mode' in config['logging'] and config['logging']["logging_mode"] == "DEBUG":
        loglevel = logging.DEBUG

    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)

    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('posttroll').setLevel(loglevel)

    LOGGER = logging.getLogger('pytroll-run-command')

    return LOGGER, handler


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
    LOGGER.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOGGER.warning("Nothing to reset/release - " +
                       "Register didn't contain any entry matching: " +
                       str(key))

    return


def terminate_process(popen_obj, scene):
    """Terminate a Popen process"""
    if popen_obj.returncode is None:
        popen_obj.kill()
        LOGGER.info("Process timed out and pre-maturely terminated. Scene: " + str(scene))
    else:
        LOGGER.info("Process finished before time out - workerScene: " + str(scene))
    return


def get_outputfiles_from_stdout(stdout, config):

    import re
    result_files = {}
    default_match = ["Start\scompressing\sand\swriting\s(.*)\s\.\.\.", ]
    if 'stdout-match' in config:
        match_list = config['stdout-match']
    else:
        match_list = default_match

    for line in stdout:
        try:
            for mtch in match_list:
                match = re.search(mtch, line.decode('utf-8'))
                if match:
                    LOGGER.debug("Matching filename: {}".format(match.group(1)))
                    if match.group(1) in result_files:
                        result_files[match.group(1)] += 1
                    else:
                        result_files[match.group(1)] = 1
        except UnicodeDecodeError as ude:
            LOGGER.error("Can not decode this line: {}. Skipping this line.".format(line))
            LOGGER.error("Fail is {}".format(ude))
    return result_files


def command_handler(semaphore_obj, config, job_dict, job_key, publish_q, input_msg, command_name, service_name_publisher):

    try:
        LOGGER.debug("Waiting for acquired semaphore...")
        with semaphore_obj:
            LOGGER.debug("Acquired semaphore")
            stdout = []
            stderr = []
            threads__ = []
            # out_readers = []
            # err_readers = []

            aliases = {}

            for key in config:
                if 'alias' in key:
                    alias = config[key]
                    new_key = key.replace('alias_', '')
                    aliases[new_key] = alias
            LOGGER.debug("alias: {}".format(aliases))

            # replace values with corresponding aliases, if any are given
            if aliases:
                info = input_msg.data.copy()
                for key in info:
                    if key in aliases:
                        try:
                            input_msg.data['orig_' + key] = input_msg.data[key]
                            input_msg.data[key] = aliases[key][str(input_msg.data[key])]
                        except KeyError as ke:
                            LOGGER.error("Key: {}, is missing in aliases.".format(key))
                            LOGGER.warning("Keep original.")
                            pass

            if 'write-message-data-to-file' in config:
                with open(config['write-message-data-to-file'], 'w') as message_data_file:
                    LOGGER.debug("Writing message data to file: %s", str(config['write-message-data-to-file']))
                    json.dump(input_msg.data, message_data_file, default=str)

            time_at_command_start = datetime.utcnow()
            for command in config['command']:
                try:
                    cmd = compose(command, input_msg.data)
                except KeyError as ke:
                    LOGGER.error("Failed to compose command: {} from input type: {} and data: {}. {}".format(
                        command, input_msg.type, input_msg.data, ke))
                    LOGGER.error("Please check your command.")
                    continue
                cmd_proc = None
                try:
                    import shlex
                    myargs = shlex.split(str(cmd))
                    LOGGER.debug('Command sequence= ' + str(myargs))
                    my_env = None
                    my_cwd = None
                    if 'pass_running_env' in config and config['pass_running_env']:
                        my_env = os.environ
                    if 'environment' in config:
                        for key in config['environment']:
                            if my_env and key in my_env:
                                # Prepend this new environment
                                if config['environment'][key] not in my_env[key]:
                                    my_env[key] = config['environment'][key] + ":" + my_env[key]
                            else:
                                # Does not exists, just set
                                if not my_env:
                                    my_env = {}
                                my_env[key] = config['environment'][key]
                    if 'working_directory' in config:
                        my_cwd = config['working_directory']
                    if 'working_directory_mkdtemp' in config:
                        my_cwd = config['working_directory_mkdtemp']
                        import tempfile
                        LOGGER.debug("About to make temp dir in : {}".format(my_cwd))
                        my_cwd = tempfile.mkdtemp(dir=my_cwd)
                        LOGGER.debug("working_directory_mkdtemp: my_cwd: {}".format(my_cwd))
                    LOGGER.debug("Complete command setup. Start running:")
                    cmd_proc = Popen(myargs, env=my_env, shell=False, stderr=PIPE, stdout=PIPE, cwd=my_cwd)
                except FileNotFoundError as fnfe:
                    LOGGER.exception("Failed in command... {}".format(fnfe))
                except Exception:
                    LOGGER.error("Failed in command... {}".format(sys.exc_info()))

                if cmd_proc:
                    process_max_run_time_seconds = 20 * 60.0
                    if 'process_max_run_time_seconds' in config:
                        process_max_run_time_seconds = config['process_max_run_time_seconds']
                    t__ = threading.Timer(process_max_run_time_seconds, terminate_process, args=(cmd_proc, config, ))
                    threads__.append(t__)
                    t__.start()

                    out_reader = threading.Thread(target=logreader, args=(cmd_proc.stdout, LOGGER.info, stdout))
                    err_reader = threading.Thread(target=logreader, args=(cmd_proc.stderr, LOGGER.info, stderr))
                    # out_readers.append(out_reader)
                    # err_readers.append(err_reader)
                    out_reader.start()
                    err_reader.start()

                    out_reader.join()
                    err_reader.join()
                    LOGGER.info("Ready with command run.")

                if 'working_directory_mkdtemp' in config and my_cwd:
                    import shutil
                    LOGGER.debug("About to remove temp dir: {}".format(my_cwd))
                    shutil.rmtree(my_cwd)
                    LOGGER.debug("removed: {}".format(my_cwd))

            # for out_reader__ in out_readers:
            #    out_reader__.join()
            # for err_reader__ in err_readers:
            #    err_reader__.join()
            time_at_command_end = datetime.utcnow()

            command_run_time = time_at_command_end - time_at_command_start
            try:
                scrape_directory = '/opt/ne/lib/node_exporter/'
                if 'scrape_directory' in config:
                    scrape_directory = config['scrape_directory']
                # Will check here. This way user can turn off scraping by setting this to None
                if scrape_directory:
                    registry = CollectorRegistry()
                    g = Gauge('command_run_time', 'Run time of command', ['command_name', 'host', 'service_name'],
                              registry=registry)
                    g.labels(command_name=command_name, host=input_msg.host,
                             service_name=service_name_publisher).set(int(command_run_time.total_seconds()))
                    write_to_textfile(os.path.join(scrape_directory,
                                                   'command_run_time-{}.prom'.format(command_name)), registry)
                    registry_lt = CollectorRegistry()
                    last_start_time = Gauge('command_start_time', 'Start time of command',
                                            ['command_name', 'host', 'service_name'],
                                            registry=registry_lt)
                    last_start_time.labels(command_name=command_name,
                                           host=input_msg.host, service_name=service_name_publisher).set(int(time_at_command_start.strftime("%s")))
                    write_to_textfile(os.path.join(scrape_directory,
                                                   'last_start_time-{}.prom'.format(command_name)), registry_lt)
            except Exception as e:
                LOGGER.error("FAILED to generate and/or write prometheus prom file(s) %s", str(e))
                pass

            if 'post_log_file' in config and os.path.exists(config['post_log_file']):
                LOGGER.debug("Will get output from post log file: %s", str(config['post_log_file']))
                with open(config['post_log_file']) as f:
                    stdout = f.readlines()
            else:
                stdout.extend(stderr)
            result_files = get_outputfiles_from_stdout(stdout, config)

            if 'publish-all-files-as-collection' in config and config['publish-all-files-as-collection']:
                LOGGER.debug("publish all file as collection")
                files = []
                for result_file, number in six.iteritems(result_files):
                    file_list = {}
                    if not os.path.exists(result_file):
                        LOGGER.error("File {} does not exists after production. Do not publish.".format(result_file))
                        continue
                    file_list['uri'] = result_file
                    filename = os.path.split(result_file)[1]
                    LOGGER.info("file to publish = " + str(filename))
                    file_list['uid'] = filename
                    files.append(file_list)

                if files:
                    to_send = input_msg.data.copy()
                    to_send.pop('dataset', None)
                    to_send.pop('collection', None)
                    to_send.pop('filename', None)
                    to_send.pop('compress', None)
                    to_send.pop('tst', None)
                    to_send.pop('uri', None)
                    to_send.pop('uid', None)
                    to_send.pop('file_list', None)
                    to_send.pop('path', None)
                    to_send['collection'] = files

                    pubmsg = Message(config['publish-topic'], "collection", to_send).encode()
                    LOGGER.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
                else:
                    LOGGER.warning("Found no files after run command. No files to publish.")

            elif len(result_files):
                # Now publish:
                for result_file, number in six.iteritems(result_files):
                    if not os.path.exists(result_file):
                        if 'working_directory' in config:
                            my_cwd = config['working_directory']
                            if os.path.exists(os.path.join(my_cwd, result_file)):
                                result_file = os.path.join(my_cwd, result_file)
                            else:
                                LOGGER.error("working_directory: File {} does not exits after production. Do not publish.".format(os.path.join(my_cwd, result_file)))
                                continue
                        else:
                            LOGGER.error("File {} does not exits after production. Do not publish.".format(result_file))
                            continue

                    filename = os.path.split(result_file)[1]
                    LOGGER.info("file to publish = " + str(filename))

                    to_send = input_msg.data.copy()
                    to_send.pop('dataset', None)
                    to_send.pop('collection', None)
                    to_send.pop('filename', None)
                    to_send.pop('compress', None)
                    to_send.pop('tst', None)
                    to_send.pop('file_list', None)
                    to_send.pop('path', None)

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
                    LOGGER.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
            else:
                LOGGER.info("No matching files to publish")

            for thread__ in threads__:
                LOGGER.debug("Canceling thread: {}".format(thread__))
                thread__.cancel()

    except:
        LOGGER.error('Failed in command_handler...')
        raise


def ready2run(msg, job_register, sceneid):
    LOGGER.debug("Scene identifier = " + str(sceneid))
    LOGGER.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOGGER.debug("Processing of scene " + str(sceneid) +
                     " have already been launched...")
        return False

    job_register[sceneid] = datetime.utcnow()

    return True


def reload_config(filename, chains,
                  listener_queue,
                  publisher_queue):
    """Rebuild chains if needed (if the configuration changed) from *filename*.
    """

    LOGGER.debug("New config file detected! " + filename)

    new_chains = read_config(filename)

    # setup new chains

    for key, val in new_chains.items():
        if key == 'logging':
            continue
        identical = True
        LOGGER.debug("key: {}, val: {}".format(key, val))
        if key in chains:
            for key2, val2 in new_chains[key].items():
                if ((key2 not in ["listeners"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    break
            if identical:
                continue

            chains[key]["listeners"].stop()
            del chains[key]["listeners"]

        chains[key] = val
        chains[key].setdefault("listeners", {})
        try:
            chains[key]["listeners"] = FileListener(listener_queue, chains[key], key)
            chains[key]["listeners"].start()

        except KeyError as err:
            LOGGER.error(str(err))
            raise
        except Exception as err:
            LOGGER.error(str(err))
            raise

        if not identical:
            LOGGER.debug("Updated " + key)
        else:
            LOGGER.debug("Added " + key)

    # disable old chains

    for key in (set(chains.keys()) - set(new_chains.keys())):
        for listener in six.iteritems(chains[key]["listeners"]):
            listener.stop()
            del chains[key]["listeners"]

        del chains[key]
        LOGGER.debug("Removed " + key)

    LOGGER.debug("Reloaded config from " + filename)


def main():
    while running:
        time.sleep(1)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config-file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-s", "--service-name-publisher",
                        help="The service name to register at the nameserver.",
                        default='run_command')
    parser.add_argument("--number-of-semaphores",
                        type=int,
                        help="Number of semaphores.",
                        dest='number_of_semaphores',
                        default=15)
    parser.add_argument("-n", "--nameservers",
                        type=str,
                        dest='nameservers',
                        default=None,
                        nargs='*',
                        help="nameservers, defaults to localhost")
    cmd_args = parser.parse_args()

    # Set up logging
    try:
        LOGGER, handler = setup_logging(cmd_args.config_file, cmd_args.log)
    except:
        print("Logging setup failed. Check your config")

    pyinotify.log.handlers = [handler]

    LOGGER.info("Starting up.")

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)

    watchman = pyinotify.WatchManager()

    listener_q = queue.Queue()
    publisher_q = queue.Queue()
    sema = threading.Semaphore(cmd_args.number_of_semaphores)

    queue_handler = threading.Thread(target=read_from_queue, args=((listener_q), cmd_args.service_name_publisher))
    queue_handler.daemon = True
    queue_handler.start()

    publisher = FilePublisher(publisher_q, cmd_args.nameservers, cmd_args.service_name_publisher)
    publisher.start()

    jobs_dict = {}
    threads = []

    def reload_cfg_file(filename, *args, **kwargs):
        reload_config(filename, chains, *args, listener_queue=listener_q, publisher_queue=publisher_q, **kwargs)

    notifier = pyinotify.ThreadedNotifier(watchman, EventHandler(reload_cfg_file, filename=cmd_args.config_file))
    watchman.add_watch(cmd_args.config_file, mask)

    def chains_stop(*args):
        global running
        LOGGER.info("Running chains stop ...")
        running = False
        notifier.stop()
        try:
            terminate(chains)
        except SystemExit:
            pass

    signal.signal(signal.SIGTERM, chains_stop)

    def signal_reload_cfg_file(*args):
        reload_config(cmd_args.config_file, chains, listener_queue=listener_q, publisher_queue=publisher_q)

    signal.signal(signal.SIGHUP, signal_reload_cfg_file)

    notifier.start()

    try:
        LOGGER.debug("Before reload cfg file at startup")
        reload_cfg_file(cmd_args.config_file)
        LOGGER.debug("Done first reload")
        shutdown = False
        while running:
            time.sleep(1)
            if not queue_handler.is_alive():
                LOGGER.error("Queue handler thread is not running")
                shutdown = True
                break
            if not publisher.is_alive():
                LOGGER.error("File Publisher thread is not running")
                shutdown = True
                break
                # main()
            for ch in chains:
                if 'listener' in ch and not ch['listener'].is_alive():
                    LOGGER.error("One of the chains are not running. Shutdown. %s", str(ch))
                    shutdown = True
                    break
            if shutdown:
                break
        LOGGER.debug("After main")
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    except:
        LOGGER.error('wow')
    finally:
        LOGGER.debug("In finally")
        if running:
            LOGGER.debug("Run Chains stop")
            chains_stop()
            LOGGER.debug("Pass kill to the queue")
            listener_q.put("kill")
            LOGGER.debug("Run queue handler join")
            queue_handler.join()
            LOGGER.debug("Queue handler complete join")
    exit_value = 0
    if shutdown:
        exit_value = 1
        LOGGER.debug("Need to shutdown.")
        if queue_handler.is_alive():
            LOGGER.debug("Shutdown queue_handler.")
            queue_handler.join()
        if publisher.is_alive():
            LOGGER.debug("Shutdown publisher.")
            publisher.stop()
        if running:
            LOGGER.debug("Shutdown chains.")
            chains_stop()
        LOGGER.debug("Shutdown complete.")

    sys.exit(exit_value)
