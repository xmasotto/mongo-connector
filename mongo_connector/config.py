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

import optparse
import copy
import json
from mongo_connector import constants

DEFAULT_CONFIG = {
    'mainAddress': 'localhost:27217',
    'oplogFile': 'oplog.txt',
    'noDump': False,
    'batchSize': constants.DEFAULT_BATCH_SIZE,
    'uniqueKey': '_id',
    'adminUser': '__system',
    'syslog': {
        'enabled': False,
        'syslogHost': 'localhost:514',
        'syslogFacility': 'user'
    },
    'autoCommitInterval': constants.DEFAULT_COMMIT_INTERVAL,
    'continueOnError': False,
    'verbose': False,
}

class Config(object):
    def __init__(self):
        self.config = copy.deepcopy(DEFAULT_CONFIG)

    def merge_dicts(self, left, right):
        for k in right:
            if type(right[k]) == dict \
               and k in left and type(left[k]) == dict:
                    self.merge_dicts(left[k], right[k])
            else:
                left[k] = right[k]

    def add_config_json(self, json):
        parsed_config = json.loads(json)
        self.merge_dicts(self.config, parsed_config)

    def add_options(self, options):
        # Translate command line configuration
        if options.main_addr:
            self.config['mainAddress'] = options.main_addr
        if options.oplog_config:
            self.config['oplogFile'] = options.oplog_config
        if options.no_dump:
            self.config['noDump'] = True
        if options.batch_size:
            self.config['batchSize'] = options.batch_size,
        if options.u_key:
            self.config['uniqueKey'] = options.u_key
        if options.auth_file:
            self.config['passwordFile'] = options.auth_file
        if options.password:
            self.config['password'] = options.password
        if options.admin_name:
            self.config['adminUser'] = options.admin_name
        if options.enable_syslog:
            self.config['syslog']['enabled'] = options.enable_syslog
        if options.syslog_host:
            self.config['syslog']['syslogHost'] = options.syslog_host
        if options.syslog_facility:
            self.config['syslog']['syslogFacility'] = options.syslog_facility
        if options.commit_interval:
            self.config['autoCommitInterval'] = options.commit_interval
        if options.continue_on_error:
            self.config['continueOnError'] = options.continue_on_error
        if options.verbose:
            self.config['verbose'] = True
        if options.logfile:
            self.config['logFile'] = options.logFile

        if 'docManagers' in self.config:
            if options.urls or options.ns_set or options.doc_managers \
               or options.dest_ns_set or options.fields:
                raise Exception("Doc Managers settings in the configuration"
                                " file cannot be overwritten by command line"
                                " arguments.")
        else:
            # Create doc managers from command line arguments
            config['docManagers'] = []

            urls = options.urls.split(",") if options.urls else []
            doc_managers = options.doc_managers.split(",") \
                           if options.doc_managers else []

            if len(urls) > 0 and len(doc_managers) == 0:
                raise Exception("Cannot create a Connector with a target URL"
                                " but no doc manager!")
            
            # target_urls may be shorter than doc_managers
            for i, d in enumerate(doc_managers)
                doc_manager = {'docManager': d}
                if i < len(urls):
                    doc_manager['targetUrl'] = urls[i]
                config['docManagers'].append(doc_manager)

            # If more target URLS were given than doc managers, may
            # need to create additional doc managers
            for url in urls[i+1:]:
                doc_manager = {'docManager': doc_managers[-1]}
                doc_manager['targetUrl'] = url
                config['docManagers'].append(doc_manager)

            if options.ns_set:
                ns_set = options.ns_set.split(',')
                if len(ns_set) != len(set(ns_set)):
                    raise Exception("Namespace set should not contain"
                                    " any duplicates!")
                for doc_manager in config['docManagers']:
                    doc_manager['includeNamespaces'] = options.ns_set

            if options.dest_ns_set:
                dest_ns_set = options.dest_ns_set.split(',')
                if len(dest_ns_set) != len(set(dest_ns_set)):
                    raise Exception("Destination namespace set should not contain"
                                    " any duplicates!")
                if not options.ns_set or len(dest_ns_set) != len(options.ns_set):
                    raise Exception("Destination namespace set should be the"
                                    " same length as the origin namespace set")
                mapping = dict(zip(ns_set, dest_ns_set))
                for doc_manager in config['docManagers']:
                    doc_manager['destMapping'] = mapping
            
            if options.fields:
                fields = options.fields.split(',')
                for doc_manager in config['docManagers']:
                    doc_manager['fields'] = fields

    def validate(self):
        # if no doc_manager is specified, use the simulator
        if len(self.config['docMangers']) == 0:
            self.config['docManagers'] = [{
                'docManager': 'doc_managers/doc_manager_simulator'
            }]

        # TODO
        # check for things... throw exceptions if they're out of order
    
