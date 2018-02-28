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
import time
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

import pyinotify
import yaml
import argparse
import signal

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
            print "Failed reading yaml config file: {} with: {}".format(filename, exc)
            raise yaml.YAMLError

    return config

def terminate(chains):
    for chain in chains.itervalues():
        LOGGER.debug("Terminate on chain: {}".format(chain))
        for listener in chain["listeners"].values():
            LOGGER.debug("stop on listener: {}".format(listener))
            listener.stop()
    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/write-to-schedule-db."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)

def read_from_queue(queue):
    #read from queue
    while True:
        LOGGER.debug("Start reading from queue ... ")
        msg_data = queue.get()
        if msg_data == None:
            LOGGER.debug("msg is none ... ")
            continue
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
                                                             msg))
        threads.append(t__)
        t__.start()
        LOGGER.debug("command handler thread object: {}".format(t__))
        LOGGER.debug("Number of threads currently alive: " + str(threading.active_count()))
        LOGGER.debug("Thread objects alive: " + str(threading.enumerate()))

        # Block any future run on this scene for x minutes from now
        # x = 20
        thread_job_registry = threading.Timer(20 * 60.0, reset_job_registry, args=(jobs_dict, keyname))
        thread_job_registry.start()

#Event handler. Needed to handle reload of config
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
            self.loop = True
            service_name = 'run_command_' + self.command_name
            LOGGER.debug("Using service_name: {}".format(service_name))
            with Publish(service_name, 0, [self.config['publish-topic'], ], nameservers=None) as publisher:

                while self.loop:
                    retv = self.queue.get()

                    if retv != None:
                        LOGGER.info("Publish the files...")
                        publisher.send(retv)

        except KeyboardInterrupt as ki:
            LOGGER.info("Received keyboard interrupt. Shutting down")
        #finally:
        #    LOGGER.info("Exiting publisher in run-command. See ya")

class FileListener(threading.Thread):

    def __init__(self, queue, config, provider, command_name):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config
        self.provider = "tcp://" + provider
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
                        #LOGGER.debug("Self.loop false in FileListener {}".format(self.loop))
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
            LOGGER.info("Some key error. probably in config")
            raise
        except KeyboardInterrupt as ki:
            LOGGER.info("Received keyboard interrupt. Shutting down")
            raise
        #finally:
        #    LOGGER.info("Exiting subscriber in run-command. See ya")

    def check_message(self, msg):

        if not msg:
            return False

        if 'providing-server' in self.config:
            if msg.host not in self.config['providing-server']:
                LOGGER.debug("Not the providing server. Providing must be: {} while message is from {}.".format(self.config['providing-server'],msg.host))
                LOGGER.debug("Skip this.");
                return False
                
        if 'sensor' in self.config:
            if 'sensor' in msg.data:
                LOGGER.debug("Check sensor.")
                if self.config['sensor'] in msg.data['sensor']:
                    LOGGER.debug("Sensor match.")
                else:
                    LOGGER.debug("Not Sensor match. Skip this.")
                    LOGGER.debug("config: {}, message: {}".format(self.config['sensor'],msg.data['sensor']))
                    return False
            else:
                LOGGER.debug("Sensor not in message. Skip this.")
                return False
        else:
            LOGGER.debug("Sensor not in config. Skip this.")
            return False
        
        if 'collection_area_id' in self.config:
            if 'collection_area_id' in msg.data:
                LOGGER.debug("Check collection area id.")
                if self.config['collection_area_id'] in msg.data['collection_area_id']:
                    LOGGER.debug("collection area id match: {}".format(self.config['collection_area_id']))
                else:
                    LOGGER.debug("No collection area id match. Skip this.")
                    LOGGER.debug("config: {}, message: {}".format(self.config['collection_area_id'],msg.data['collection_area_id']))
                    return False
            else:
                LOGGER.debug("collection_area_id not in message. Skip this.")
                return False
        else:
            LOGGER.debug("collection_area_id not in config. Process anyway.")
                
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
                                LOGGER.error("Path differs from previous path. This will cause problems.")
                                LOGGER.warning("previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOGGER.debug("Path is {}".format(msg.data['path']))
                    else:
                        LOGGER.error("URI not found in dataset")
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
                                LOGGER.error("Path differs from previous path. This will cause problems.")
                                LOGGER.warning("previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
                                return False
                        else:
                            msg.data['path'] = os.path.dirname(urlobj.path)
                        LOGGER.debug("Path is {}".format(msg.data['path']))
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
                                        LOGGER.warning("Path differs from previous path. This can cause problems if 'path' keyword is used.")
                                        LOGGER.warning("Keeping previous path: {}, this path is : {}".format(msg.data['path'],os.path.dirname(urlobj.path)))
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
                    LOGGER.debug("Resolution config and message don't match up: {} vs {}".format(self.config['resolution'],msg.data['resolution']))
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
        except KeyError as err:
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
    if popen_obj.returncode == None:
        popen_obj.kill()
        LOGGER.info("Process timed out and pre-maturely terminated. Scene: " + str(scene))
    else:
        LOGGER.info("Process finished before time out - workerScene: " + str(scene))
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
                LOGGER.debug("Matching filename: {}".format(match.group(1)))
                if match.group(1) in result_files:
                    result_files[match.group(1)]+=1
                else:
                    result_files[match.group(1)] = 1
            
    return result_files

def command_handler(semaphore_obj, config, job_dict, job_key, publish_q, input_msg):

    try:
        LOGGER.debug("Waiting for acquired semaphore...")
        with semaphore_obj:
            LOGGER.debug("Acquired semaphore")
            stdout = []
            stderr = []
            threads__ = []
            #out_readers = []
            #err_readers = []

            for command in config['command']:
                try:
                    cmd = compose(command,input_msg.data)
                except KeyError as ke:
                    LOGGER.error("Failed to compose command: {} from input data: {}".format(command, input_msg.data))
                    LOGGER.error("Please check your command.")
                    continue
                try:
                    import shlex
                    myargs = shlex.split(str(cmd))
                    LOGGER.debug('Command sequence= ' + str(myargs))
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
                    LOGGER.exception("Failed in command... {}".format(sys.exc_info()))

                t__ = threading.Timer(20 * 60.0, terminate_process, args=(cmd_proc, config, ))
                threads__.append(t__)
                t__.start()

                out_reader = threading.Thread(target=logreader, args=(cmd_proc.stdout, LOGGER.info, stdout))
                err_reader = threading.Thread(target=logreader, args=(cmd_proc.stderr, LOGGER.info, stderr))
                #out_readers.append(out_reader)
                #err_readers.append(err_reader)
                out_reader.start()
                err_reader.start()

                out_reader.join()
                err_reader.join()
                LOGGER.info("Ready with command run.")

            #for out_reader__ in out_readers:
            #    out_reader__.join()
            #for err_reader__ in err_readers:
            #    err_reader__.join()


            result_files = get_outputfiles_from_stdout(stdout, config)
            
            if 'publish-all-files-as-collection' in config and config['publish-all-files-as-collection']:
                LOGGER.debug("publish all file as collection")
                files = []
                for result_file,number in result_files.iteritems():
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
                    to_send.pop('filename',None)
                    to_send.pop('compress',None)
                    to_send.pop('tst',None)
                    to_send.pop('uri',None)
                    to_send.pop('uid',None)
                    to_send['collection'] = files

                    pubmsg = Message(config['publish-topic'], "collection", to_send).encode()
                    LOGGER.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
                else:
                    LOGGER.warning("Found no files after run command. No files to publish.")

            elif len(result_files):
                # Now publish:
                for result_file,number in result_files.iteritems():
                    if not os.path.exists(result_file):
                        LOGGER.error("File {} does not exits after production. Do not publish.".format(result_file))
                        continue

                    filename = os.path.split(result_file)[1]
                    LOGGER.info("file to publish = " + str(filename))

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
                    LOGGER.info("Sending: " + str(pubmsg))
                    publish_q.put(pubmsg)
            else:
                LOGGER.info("No matching files to publish")

            for thread__ in threads__:
                LOGGER.debug("Canceling thread: {}".format(thread__))
                thread__.cancel()

    except:
        LOGGER.exception('Failed in command_handler...')
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
        LOGGER.debug("key: {}, val: {}".format( key,val))
        if key in chains:
            for key2, val2 in new_chains[key].items():
                if ((key2 not in ["listeners", "publisher"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    break
            if identical:
                continue

            if "providers" in chains[key]:
                for provider in chains[key]["providers"]:
                    chains[key]["listeners"][provider].stop()
                    del chains[key]["listeners"][provider]
                    chains[key]["publisher"][provider].stop()
                    del chains[key]["publisher"][provider]
            else:
                continue

        chains[key] = val
        chains[key].setdefault("listeners", {})
        chains[key].setdefault("publisher", {})
        try:
            for provider in chains[key]["providers"]:
                chains[key]["listeners"][provider] = FileListener(listener_queue, chains[key], provider, key)
                chains[key]["listeners"][provider].start()
                #chains[key]["publisher"] = publisher_queue
                chains[key]["publisher"][provider] = FilePublisher(publisher_queue, chains[key], key)
                chains[key]["publisher"][provider].start()

        except KeyError as err:
            LOGGER.exception(str(err))
            raise
        except Exception as err:
            LOGGER.exception(str(err))
            raise
        
        if not identical:
            LOGGER.debug("Updated " + key)
        else:
            LOGGER.debug("Added " + key)

    # disable old chains

    for key in (set(chains.keys()) - set(new_chains.keys())):
        for provider, listener in chains[key]["listeners"].iteritems():
            listener.stop()
            del chains[key]["listeners"][provider]

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
    cmd_args = parser.parse_args()

    #Set up logging
    try:
        LOGGER, handler = setup_logging(cmd_args.config_file, cmd_args.log)
    except:
        print "Logging setup failed. Check your config"

    pyinotify.log.handlers = [handler]

    LOGGER.info("Starting up.")

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)

    watchman = pyinotify.WatchManager()

    listener_q = Queue.Queue()
    publisher_q = Queue.Queue()
    sema = threading.Semaphore(5)

    queue_handler = threading.Thread(target=read_from_queue, args=((listener_q),))
    queue_handler.daemon=True
    queue_handler.start()

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
        LOGGER.debug("Befire reload cfg file at startup")
        reload_cfg_file(cmd_args.config_file)
        LOGGER.debug("Done first reload")
        main()
        LOGGER.debug("After main")
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    except:
        LOGGER.exception('wow')
    finally:
        if running:
            chains_stop()
            queue_handler.join()


    sys.exit(0)

