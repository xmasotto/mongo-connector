# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Discovers the mongo cluster and starts the connector.
"""

import json
import logging
import logging.handlers
import os
import pymongo
import pprint
import re
import shutil
import sys
import threading
import time
import imp
from mongo_connector import config, constants, errors, util
from mongo_connector.compat import zip_longest
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.doc_managers import (
    DocManagerBase,
    doc_manager_simulator as simulator)

from pymongo import MongoClient

LOG = logging.getLogger('mongo-connector')
LOG.addHandler(logging.NullHandler())


class Connector(threading.Thread):
    """Checks the cluster for shards to tail.
    """
    def __init__(self, address, oplog_checkpoint, ns_set,
                 auth_key, doc_managers=None, auth_username=None,
                 collection_dump=True, batch_size=constants.DEFAULT_BATCH_SIZE,
                 fields=None, dest_mapping={},
                 auto_commit_interval=constants.DEFAULT_COMMIT_INTERVAL,
                 continue_on_error=False):

        super(Connector, self).__init__()

        # can_run is set to false when we join the thread
        self.can_run = True

        # The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = oplog_checkpoint

        # main address - either mongos for sharded setups or a primary otherwise
        self.address = address

        # The set of relevant namespaces to consider
        self.ns_set = ns_set

        # The dict of source namespace to destination namespace
        self.dest_mapping = dest_mapping

        # Whether the collection dump gracefully handles exceptions
        self.continue_on_error = continue_on_error

        # Password for authentication
        self.auth_key = auth_key

        # List of DocManager instances
        if doc_managers:
            self.doc_managers = doc_managers
        else:
            self.doc_managers = (simulator.DocManager(),)

        # Username for authentication
        self.auth_username = auth_username

        # The set of OplogThreads created
        self.shard_set = {}

        # Boolean chooses whether to dump the entire collection if no timestamp
        # is present in the config file
        self.collection_dump = collection_dump

        # Num entries to process before updating config file with current pos
        self.batch_size = batch_size

        # Dict of OplogThread/timestamp pairs to record progress
        self.oplog_progress = LockingDict()

        # List of fields to export
        self.fields = fields

        # Doc Managers
        self.doc_managers = doc_managers

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = ("MongoConnector: Can't find %s, "
                            "attempting to create an empty progress log" %
                            self.oplog_checkpoint)
                logging.info(info_str)
                try:
                    # Create oplog progress file
                    open(self.oplog_checkpoint, "w").close()
                except IOError as e:
                    logging.critical("MongoConnector: Could not "
                                     "create a progress log: %s" %
                                     str(e))
                    sys.exit(2)
            else:
                if (not os.access(self.oplog_checkpoint, os.W_OK)
                        and not os.access(self.oplog_checkpoint, os.R_OK)):
                    logging.critical("Invalid permissions on %s! Exiting" %
                                     (self.oplog_checkpoint))
                    sys.exit(2)

    def join(self):
        """ Joins thread, stops it from running
        """
        self.can_run = False
        for dm in self.doc_managers:
            dm.stop()
        threading.Thread.join(self)

    def write_oplog_progress(self):
        """ Writes oplog progress to file provided by user
        """

        if self.oplog_checkpoint is None:
            return None

        # write to temp file
        backup_file = self.oplog_checkpoint + '.backup'
        os.rename(self.oplog_checkpoint, backup_file)

        # for each of the threads write to file
        with open(self.oplog_checkpoint, 'w') as dest:
            with self.oplog_progress as oplog_prog:

                oplog_dict = oplog_prog.get_dict()
                for oplog, time_stamp in oplog_dict.items():
                    oplog_str = str(oplog)
                    timestamp = util.bson_ts_to_long(time_stamp)
                    json_str = json.dumps([oplog_str, timestamp])
                    try:
                        dest.write(json_str)
                    except IOError:
                        # Basically wipe the file, copy from backup
                        dest.truncate()
                        with open(backup_file, 'r') as backup:
                            shutil.copyfile(backup, dest)
                        break

        os.remove(self.oplog_checkpoint + '.backup')

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user.
        This method is only called once before any threads are spanwed.
        """

        if self.oplog_checkpoint is None:
            return None

        # Check for empty file
        try:
            if os.stat(self.oplog_checkpoint).st_size == 0:
                logging.info("MongoConnector: Empty oplog progress file.")
                return None
        except OSError:
            return None

        source = open(self.oplog_checkpoint, 'r')
        try:
            data = json.load(source)
        except ValueError:       # empty file
            reason = "It may be empty or corrupt."
            logging.info("MongoConnector: Can't read oplog progress file. %s" %
                         (reason))
            source.close()
            return None

        source.close()

        count = 0
        oplog_dict = self.oplog_progress.get_dict()
        for count in range(0, len(data), 2):
            oplog_str = data[count]
            time_stamp = data[count + 1]
            oplog_dict[oplog_str] = util.long_to_bson_ts(time_stamp)
            #stored as bson_ts

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        main_conn = MongoClient(self.address)
        if self.auth_key is not None:
            main_conn['admin'].authenticate(self.auth_username, self.auth_key)
        self.read_oplog_progress()
        conn_type = None

        try:
            main_conn.admin.command("isdbgrid")
        except pymongo.errors.OperationFailure:
            conn_type = "REPLSET"

        if conn_type == "REPLSET":
            # Make sure we are connected to a replica set
            is_master = main_conn.admin.command("isMaster")
            if not "setName" in is_master:
                logging.error(
                    'No replica set at "%s"! A replica set is required '
                    'to run mongo-connector. Shutting down...' % self.address
                )
                return

            # Establish a connection to the replica set as a whole
            main_conn.disconnect()
            main_conn = MongoClient(self.address,
                                    replicaSet=is_master['setName'])
            if self.auth_key is not None:
                main_conn.admin.authenticate(self.auth_username, self.auth_key)

            #non sharded configuration
            oplog_coll = main_conn['local']['oplog.rs']

            oplog = OplogThread(
                primary_conn=main_conn,
                main_address=self.address,
                oplog_coll=oplog_coll,
                is_sharded=False,
                doc_managers=self.doc_managers,
                oplog_progress_dict=self.oplog_progress,
                namespace_set=self.ns_set,
                auth_key=self.auth_key,
                auth_username=self.auth_username,
                repl_set=is_master['setName'],
                collection_dump=self.collection_dump,
                batch_size=self.batch_size,
                fields=self.fields,
                dest_mapping=self.dest_mapping,
                continue_on_error=self.continue_on_error
            )
            self.shard_set[0] = oplog
            logging.info('MongoConnector: Starting connection thread %s' %
                         main_conn)
            oplog.start()

            while self.can_run:
                if not self.shard_set[0].running:
                    logging.error("MongoConnector: OplogThread"
                                  " %s unexpectedly stopped! Shutting down" %
                                  (str(self.shard_set[0])))
                    self.oplog_thread_join()
                    for dm in self.doc_managers:
                        dm.stop()
                    return

                self.write_oplog_progress()
                time.sleep(1)

        else:       # sharded cluster
            while self.can_run is True:

                for shard_doc in main_conn['config']['shards'].find():
                    shard_id = shard_doc['_id']
                    if shard_id in self.shard_set:
                        if not self.shard_set[shard_id].running:
                            logging.error("MongoConnector: OplogThread "
                                          "%s unexpectedly stopped! Shutting "
                                          "down" %
                                          (str(self.shard_set[shard_id])))
                            self.oplog_thread_join()
                            for dm in self.doc_managers:
                                dm.stop()
                            return

                        self.write_oplog_progress()
                        time.sleep(1)
                        continue
                    try:
                        repl_set, hosts = shard_doc['host'].split('/')
                    except ValueError:
                        cause = "The system only uses replica sets!"
                        logging.error("MongoConnector: %s", cause)
                        self.oplog_thread_join()
                        for dm in self.doc_managers:
                            dm.stop()
                        return

                    shard_conn = MongoClient(hosts, replicaSet=repl_set)
                    oplog_coll = shard_conn['local']['oplog.rs']

                    oplog = OplogThread(
                        primary_conn=shard_conn,
                        main_address=self.address,
                        oplog_coll=oplog_coll,
                        is_sharded=True,
                        doc_managers=self.doc_managers,
                        oplog_progress_dict=self.oplog_progress,
                        namespace_set=self.ns_set,
                        auth_key=self.auth_key,
                        auth_username=self.auth_username,
                        collection_dump=self.collection_dump,
                        batch_size=self.batch_size,
                        fields=self.fields,
                        dest_mapping=self.dest_mapping,
                        continue_on_error=self.continue_on_error
                    )
                    self.shard_set[shard_id] = oplog
                    msg = "Starting connection thread"
                    logging.info("MongoConnector: %s %s" % (msg, shard_conn))
                    oplog.start()

        self.oplog_thread_join()
        self.write_oplog_progress()

    def oplog_thread_join(self):
        """Stops all the OplogThreads
        """
        logging.info('MongoConnector: Stopping all OplogThreads')
        for thread in self.shard_set.values():
            thread.join()

def main():
    """ Starts the mongo connector (assuming CLI)
    """
    parser = optparse.OptionParser()

def get_config_options():
    result = []

    def add_option(*args, **kwargs):
        opt = config.Option(*args, **kwargs)
        result.append(opt)
        return opt

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    main_address = add_option("mainAddress", "localhost:27017")
    main_address.set_type(str)
    main_address.add_cli(
        "-m", "--main", dest="main_address", help=
        "Specify the main address, which is a"
        " host:port pair. For sharded clusters, this"
        " should be the mongos address. For individual"
        " replica sets, supply the address of the"
        " primary. For example, `-m localhost:27217`"
        " would be a valid argument to `-m`. Don't use"
        " quotes around the address.")

    #-o is to specify the oplog-config file. This file is used by the system
    #to store the last timestamp read on a specific oplog. This allows for
    #quick recovery from failure.
    oplog_file = add_option("oplogFile", "oplog.timestamp")
    oplog_file.set_type(str)
    oplog_file.add_cli(
        "-o", "--oplog-ts", dest="oplog_file", help=
        "Specify the name of the file that stores the "
        "oplog progress timestamps. "
        "This file is used by the system to store the last "
        "timestamp read on a specific oplog. This allows "
        "for quick recovery from failure. By default this "
        "is `config.txt`, which starts off empty. An empty "
        "file causes the system to go through all the mongo "
        "oplog and sync all the documents. Whenever the "
        "cluster is restarted, it is essential that the "
        "oplog-timestamp config file be emptied - otherwise "
        "the connector will miss some documents and behave "
        "incorrectly.")

    #--no-dump specifies whether we should read an entire collection from
    #scratch if no timestamp is found in the oplog_config.
    no_dump = add_option("noDump", False)
    no_dump.set_type(bool)
    no_dump.add_cli(
        "--no-dump", action="store_true", dest="no_dump", help=
        "If specified, this flag will ensure that "
        "mongo_connector won't read the entire contents of a "
        "namespace iff --oplog-ts points to an empty file.")

    #--batch-size specifies num docs to read from oplog before updating the
    #--oplog-ts config file with current oplog position
    batch_size = add_option("batchSize", constants.DEFAULT_BATCH_SIZE)
    batch_size.set_type(int)
    batch_size.add_cli(
        "--batch-size", type="int", dest="batch_size", help=
        "Specify an int to update the --oplog-ts "
        "config file with latest position of oplog every "
        "N documents. By default, the oplog config isn't "
        "updated until we've read through the entire oplog. "
        "You may want more frequent updates if you are at risk "
        "of falling behind the earliest timestamp in the oplog")

    def apply_verbosity(option, cli_values):
        if cli_values['verbose']:
            option.value = 1
        if option.value < 0:
            raise errors.InvalidConfiguration("verbosity must be non-negative.")

    #-v enables verbose logging
    verbosity = add_option("verbosity", 0, apply_verbosity)
    verbosity.set_type(int)
    verbosity.add_cli(
        "-v", "--verbose", action="store_true",
        dest="verbose", help=
        "Sets verbose logging to be on.")

    def apply_logging(option, cli_values):
        if cli_values['logfile'] and cli_values['enable_syslog']:
            raise errors.InvalidConfiguration(
                "You cannot specify syslog and a logfile simultaneously,"
                " please choose the logging method you would prefer.")

        if cli_values['logfile']:
            option.value['type'] = 'file'
            option.value['filename'] = cli_values['logfile']

        if cli_values['enable_syslog']:
            option.value['type'] = 'syslog'

        if cli_values['syslog_host']:
            option.value['host'] = cli_values['syslog_host']

        if cli_values['syslog_facility']:
            option.value['facility'] = cli_values['syslog_facility']

    default_logging = {
        'host': constants.DEFAULT_SYSLOG_HOST,
        'facility': constants.DEFAULT_SYSLOG_FACILITY
    }

    logging = add_option("logging", default_logging, apply_logging)
    logging.set_type(dict)

    #-w enable logging to a file
    logging.add_cli(
        "-w", "--logfile", dest="logfile", help=
        "Log all output to a file rather than stream to "
        "stderr. Omit to stream to stderr.")

    #-s is to enable syslog logging.
    logging.add_cli(
        "-s", "--enable-syslog", action="store_true",
        dest="enable_syslog", help=
        "Used to enable logging to syslog."
        " Use -l to specify syslog host.")

    #--syslog-host is to specify the syslog host.
    logging.add_cli(
        "--syslog-host", dest="syslog_host", help=
        "Used to specify the syslog host."
        " The default is 'localhost:514'")

    #--syslog-facility is to specify the syslog facility.
    logging.add_cli(
        "--syslog-facility", dest="syslog_facility", help=
        "Used to specify the syslog facility."
        " The default is 'user'")

    def apply_authentication(option, cli_values):
        if cli_values['admin_username']:
            option.value['adminUsername'] = cli_values['admin_username']

        if cli_values['password']:
            option.value['password'] = cli_values['password']

        if cli_values['password_file']:
            option.value['passwordFile'] = cli_values['password_file']

        if option.value.get("adminUsername"):
            password = option.value.get("password")
            passwordFile = option.value.get("passwordFile")
            if not password and not passwordFile:
                raise errors.InvalidConfiguration(
                    "Admin username specified without password.")
            if password and passwordFile:
                raise errors.InvalidConfiguration(
                    "Can't specify both password and password file.")

    default_authentication = {
        'adminUsername': None,
        'password': None,
        'passwordFile': None
    }

    authentication = add_option("authentication",
                                default_authentication, apply_authentication)
    authentication.set_type(dict)

    #-a is to specify the username for authentication.
    authentication.add_cli(
        "-a", "--admin-username", dest="admin_username", help=
        "Used to specify the username of an admin user to "
        "authenticate with. To use authentication, the user "
        "must specify both an admin username and a keyFile.")

    #-p is to specify the password used for authentication.
    authentication.add_cli(
        "-p", "--password", dest="password", help=
        "Used to specify the password."
        " This is used by mongos to authenticate"
        " connections to the shards, and in the"
        " oplog threads. If authentication is not used, then"
        " this field can be left empty as the default ")

    #-f is to specify the authentication key file. This file is used by mongos
    #to authenticate connections to the shards, and we'll use it in the oplog
    #threads.
    authentication.add_cli(
        "-f", "--password-file", dest="password_file", help=
        "Used to store the password for authentication."
        " Use this option if you wish to specify a"
        " username and password but don't want to"
        " type in the password. The contents of this"
        " file should be the password for the admin user.")

    def apply_fields(option, cli_values):
        if cli_values['fields']:
            option.value = cli_values['fields'].split(",")

    fields = add_option("fields", [], apply_fields)
    fields.set_type(list)

    #-i to specify the list of fields to export
    fields.add_cli(
        "-i", "--fields", dest="fields", help=
        "Used to specify the list of fields to export. "
        "Specify a field or fields to include in the export. "
        "Use a comma separated list of fields to specify multiple "
        "fields. The '_id', 'ns' and '_ts' fields are always "
        "exported.")

    def apply_namespaces(option, cli_values):
        if cli_values['ns_set']:
            ns_set = cli_values['ns_set'].split(',')
            if len(ns_set) != len(set(ns_set)):
                raise errors.InvalidConfiguration(
                    "Namespace set should not contain any duplicates.")
            option.value['include'] = ns_set

        if cli_values['dest_ns_set']:
            ns_set = option.value['include']
            dest_ns_set = cli_values['dest_ns_set'].split(',')
            if len(dest_ns_set) != len(set(dest_ns_set)):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should not"
                    " contain any duplicates.")
            if len(ns_set) != len(dest_ns_set):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should be the"
                    " same length as the origin namespace set.")
            option.value['mapping'] = dict(zip(ns_set, dest_ns_set))

    default_namespaces = {
        "include": [],
        "mapping": {}
    }

    namespaces = add_option("namespaces", default_namespaces, apply_namespaces)
    namespaces.set_type(dict)

    #-n is to specify the namespaces we want to consider. The default
    #considers all the namespaces
    namespaces.add_cli(
        "-n", "--namespace-set", dest="ns_set", help=
        "Used to specify the namespaces we want to "
        "consider. For example, if we wished to store all "
        "documents from the test.test and alpha.foo "
        "namespaces, we could use `-n test.test,alpha.foo`. "
        "The default is to consider all the namespaces, "
        "excluding the system and config databases, and "
        "also ignoring the \"system.indexes\" collection in "
        "any database.")

    #-g is the destination namespace
    namespaces.add_cli(
        "-g", "--dest-namespace-set", dest="dest_ns_set", help=
        "Specify a destination namespace mapping. Each "
        "namespace provided in the --namespace-set option "
        "will be mapped respectively according to this "
        "comma-separated list. These lists must have "
        "equal length. The default is to use the identity "
        "mapping. This is currently only implemented "
        "for mongo-to-mongo connections.")

    def apply_doc_managers(option, cli_values):
        if cli_values['doc_manager'] is None:
            if cli_values['target_url']:
                raise errors.InvalidConfiguration(
                    "Cannot create a Connector with a target URL"
                    " but no doc manager.")
        else:
            option.value = [{
                'docManager': cli_values['doc_manager'],
                'targetURL': cli_values['target_url'],
                'uniqueKey': cli_values['unique_key'],
                'autoCommitInterval': cli_values['auto_commit_interval']
            }]

        if not option.value:
            LOG.info('No doc managers specified, using simulator.')
            option.value = [{
                'docManager': 'doc_manager_simulator'
            }]

        # validate doc managers and fill in default values
        for dm in option.value:
            if not isinstance(dm, dict):
                raise errors.InvalidConfiguration(
                    "Elements of docManagers must be a dict.")
            if 'docManager' not in dm:
                raise errors.InvalidConfiguration(
                    "Every element of docManagers"
                    " must contain 'docManager' property.")
            if not dm.get('targetURL'):
                dm['targetURL'] = None
            if not dm.get('uniqueKey'):
                dm['uniqueKey'] = constants.DEFAULT_UNIQUE_KEY
            if not dm.get('autoCommitInterval'):
                dm['autoCommitInterval'] = constants.DEFAULT_COMMIT_INTERVAL
            if not dm.get('args'):
                dm['args'] = {}

            if dm['autoCommitInterval'] and dm['autoCommitInterval'] < 0:
                raise errors.InvalidConfiguration(
                    "autoCommitInterval must be non-negative.")

        def import_dm_by_name(name):
            try:
                full_name = "mongo_connector.doc_managers.%s" % name
                # importlib doesn't exist in 2.6, but __import__ is everywhere
                module = __import__(full_name, fromlist=(name,))
                dm_impl = module.DocManager
                if not issubclass(dm_impl, DocManagerBase):
                    raise TypeError("DocManager must inherit DocManagerBase.")
                return module
            except ImportError:
                LOG.exception("Could not import %s." % full_name)
                sys.exit(1)
            except (AttributeError, TypeError):
                LOG.exception("No definition for DocManager found in %s."
                              % full_name)
                sys.exit(1)

        dm_instances = []
        for dm in option.value:
            module = import_dm_by_name(dm['docManager'])
            kwargs = {
                'unique_key': dm['uniqueKey'],
                'auto_commit_interval': dm['autoCommitInterval']
            }
            for k in dm['args']:
                if k not in kwargs:
                    kwargs[k] = dm['args'][k]

            target_url = dm['targetURL']
            if target_url:
                dm_instances.append(module.DocManager(target_url, **kwargs))
            else:
                dm_instances.append(module.DocManager(**kwargs))

        option.value = dm_instances

    doc_managers = add_option("docManagers", None, apply_doc_managers)
    doc_managers.set_type(list)

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-d", "--doc-manager", dest="doc_manager", help=
        "Used to specify the path to each doc manager "
        "file that will be used. DocManagers should be "
        "specified in the same order as their respective "
        "target addresses in the --target-urls option. "
        "URLs are assigned to doc managers "
        "respectively. Additional doc managers are "
        "implied to have no target URL. Additional URLs "
        "are implied to have the same doc manager type as "
        "the last doc manager for which a URL was "
        "specified. By default, Mongo Connector will use "
        "'doc_manager_simulator.py'.  It is recommended "
        "that all doc manager files be kept in the "
        "doc_managers folder in mongo-connector. For "
        "more information about making your own doc "
        "manager, see 'Writing Your Own DocManager' "
        "section of the wiki")

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-t", "--target-url",
        dest="target_url", help=
        "Specify the URL to each target system being "
        "used. For example, if you were using Solr out of "
        "the box, you could use '-t "
        "http://localhost:8080/solr' with the "
        "SolrDocManager to establish a proper connection. "
        "URLs should be specified in the same order as "
        "their respective doc managers in the "
        "--doc-managers option.  URLs are assigned to doc "
        "managers respectively. Additional doc managers "
        "are implied to have no target URL. Additional "
        "URLs are implied to have the same doc manager "
        "type as the last doc manager for which a URL was "
        "specified. "
        "Don't use quotes around addresses. ")

    # -u is to specify the mongoDB field that will serve as the unique key
    # for the target system,
    doc_managers.add_cli(
        "-u", "--unique-key", dest="unique_key", help=
        "The name of the MongoDB field that will serve "
        "as the unique key for the target system. "
        "Note that this option does not apply "
        "when targeting another MongoDB cluster. "
        "Defaults to \"_id\".")

    # --auto-commit-interval to specify auto commit time interval
    doc_managers.add_cli(
        "--auto-commit-interval", type="int",
        dest="auto_commit_interval", help=
        "Seconds in-between calls for the Doc Manager"
        " to commit changes to the target system. A value of"
        " 0 means to commit after every write operation."
        " When left unset, Mongo Connector will not make"
        " explicit commits. Some systems have"
        " their own mechanism for adjusting a commit"
        " interval, which should be preferred to this"
        " option.")

    # --continue-on-error to continue to upsert documents during a collection
    # dump, even if the documents cannot be inserted for some reason
    continue_on_error = add_option("continueOnError", False)
    continue_on_error.set_type(bool)
    continue_on_error.add_cli(
        "--continue-on-error", action="store_true",
        dest="continue_on_error", help=
        "By default, if any document fails to upsert"
        " during a collection dump, the entire operation fails."
        " When this flag is enabled, normally fatal errors"
        " will be caught and logged, allowing the collection"
        " dump to continue.\n"
        "Note: Applying oplog operations to an incomplete"
        " set of documents due to errors may cause undefined"
        " behavior. Use this flag to dump only.")

    # -c to load a config file
    config_file = add_option()
    config_file.add_cli(
        "-c", "--config-file", dest="config_file", help=
        "Specify a JSON file to load configurations from. You can find"
        " an example config file at mongo-connector/config.json")

    return result

def main():
    """ Starts the mongo connector (assuming CLI)
    """
    conf = config.Config(get_config_options())
    conf.parse_args()

    # PRINT THE CONFIGURATION
    def filter_dunder_keys(x):
        if isinstance(x, list):
            return [filter_dunder_keys(c) for c in x]
        elif isinstance(x, dict):
            result = {}
            for k in x:
                if not k.startswith('__'):
                    result[k] = filter_dunder_keys(x[k])
            return result
        else:
            return x

    print("Loading Mongo Connector with the following configuration:")
    pp = pprint.PrettyPrinter(indent=4)
    config_dict = dict(
        (opt.config_key, opt.value) for opt in conf.options if opt.config_key)
    pp.pprint(filter_dunder_keys(config_dict))
    print("")

    logger = logging.getLogger()
    loglevel = logging.DEBUG if conf['verbosity'] > 0 else logging.INFO
    logger.setLevel(loglevel)

    if conf['logging.type'] == 'file':
        log_out = logging.FileHandler(conf['logging.filename'])
        log_out.setLevel(loglevel)
        log_out.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_out)

    if conf['logging.type'] == 'syslog':
        syslog_info = conf['logging.host'].split(":")
        syslog_host = logging.handlers.SysLogHandler(
            address=(syslog_info[0], int(syslog_info[1])),
            facility=conf['logging.facility']
        )
        syslog_host.setLevel(loglevel)
        logger.addHandler(syslog_host)

    if conf['logging.type'] is None:
        log_out = logging.StreamHandler()
        log_out.setLevel(loglevel)
        log_out.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'))
        LOG.addHandler(log_out)

    logger.info('Beginning Mongo Connector')

    auth_key = None
    password_file = conf['authentication.passwordFile']
    if password_file is not None:
        try:
            auth_key = open(conf['passwordFile']).read()
            auth_key = re.sub(r'\s', '', auth_key)
        except IOError:
            LOG.error('Could not parse password authentication file!')
            sys.exit(1)
    password = conf['authentication.password']
    if password is not None:
        auth_key = password

    connector = Connector(
        address=conf['mainAddress'],
        oplog_checkpoint=conf['oplogFile'],
        collection_dump=(not conf['noDump']),
        batch_size=conf['batchSize'],
        continue_on_error=conf['continueOnError'],
        auth_username=conf['authentication.adminUsername'],
        auth_key=auth_key,
        fields=conf['fields'],
        ns_set=conf['namespaces.include'],
        dest_mapping=conf['namespaces.mapping'],
        doc_managers=conf['docManagers'],
    )
    connector.start()

    while True:
        try:
            time.sleep(3)
            if not connector.is_alive():
                break
        except KeyboardInterrupt:
            logging.info("Caught keyboard interrupt, exiting!")
            connector.join()
            break

if __name__ == '__main__':
    main()
