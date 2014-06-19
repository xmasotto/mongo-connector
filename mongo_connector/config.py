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

import copy
import json

from mongo_connector import constants, errors

class Option(object):
    def __init__(self, *names, **kwargs):
        self.default = kwargs.pop('default', None)
        self.apply_function = kwargs.pop('apply_function', None)
        self.dest = kwargs.get('dest', None)
        self.names = names
        self.kwargs = kwargs

    def add_option(parser):
        parser.add_option(*self.names, **self.kwargs)
    
    def apply(self, config, values):
        if self.apply_function:
            self.apply_function(config, values)

class SimpleOption(Option):
    def __init__(self, *names, **kwargs):
        Option.__init__(self, *names, **kwargs)
        config_key = kwargs.pop('config_key', None)

        def simple_apply(config, values):
            if config_key and self.dest:
                # allow for "nested.field.keys"
                keys = config_key.split('.')
                for key in keys[:-1]:
                    if key not in config:
                        config[key] = {}
                    config = config[key]
                config[keys[-1]] = values[self.dest]

        self.apply_function = simple_apply


class Config(object):
    def __init__(self, options):
        # initialize the default config
        self.config = {}

        defaults = {}
        for option in options:
            defaults[option.dest] = option.default

        for option in options:
            option.apply(self.config, defaults)

    def parse_args(self):
        # parse the command line options
        parser = optparse.OptionParser()
        for option in options:
            option.add_option(parser)
        parsed_options, args = parser.parse_args()

        # load the config file
        if parsed_options.config_file:
            try:
                with open(parsed_options.config_file) as f:
                    self.load_json(f.read())
            except Exception as e:
                raise errors.InvalidConfiguration(
                    "Exception occured while parsing config file: %s"
                    % e.message)

        # apply the command line arguments
        for option in options:
            values = parsed_options.__dict__
            if value:
                option.apply(self.config, values)

    def __getitem__(self, key):
        return self.config[key]

    def load_json(self, text):
        parsed_config = json.loads(text)
        self.merge_dicts(self.config, parsed_config)

    def merge_dicts(self, left, right):
        for k in right:
            if type(right[k]) == dict \
               and k in left and type(left[k]) == dict:
                    self.merge_dicts(left[k], right[k])
            else:
                left[k] = right[k]

"""
    def __init__(self):
        self.config = copy.deepcopy(constants.DEFAULT_CONFIG)


    def add_config_json(self, text):
        try:
            parsed_config = json.loads(text)
        except Exception as e:
            raise errors.InvalidConfiguration(
                "Exception occurred while parsing JSON: %s" % e.message)

        self.merge_dicts(self.config, parsed_config)

    def add_options(self, options):
        # Translate command line configuration
        if options.main_address:
            self.config['mainAddress'] = options.main_address
        if options.oplog_file:
            self.config['oplogFile'] = options.oplog_file
        if options.no_dump:
            self.config['noDump'] = True
        if options.batch_size:
            self.config['batchSize'] = options.batch_size
        if options.unique_key:
            self.config['uniqueKey'] = options.unique_key
        if options.password_file:
            self.config['passwordFile'] = options.password_file
        if options.password:
            self.config['password'] = options.password
        if options.admin_username:
            self.config['adminUsername'] = options.admin_username
        if options.auto_commit_interval:
            self.config['autoCommitInterval'] = options.auto_commit_interval
        if options.continue_on_error:
            self.config['continueOnError'] = options.continue_on_error
        if options.verbose:
            self.config['verbose'] = True
        if options.logfile:
            self.config['logFile'] = options.logfile
        if options.enable_syslog:
            self.config['syslog']['enabled'] = options.enable_syslog
        if options.syslog_host:
            self.config['syslog']['host'] = options.syslog_host
        if options.syslog_facility:
            self.config['syslog']['facility'] = options.syslog_facility

        if options.ns_set:
            ns_set = options.ns_set.split(',')
            if len(ns_set) != len(set(ns_set)):
                raise errors.InvalidConfiguration(
                    "Namespace set should not contain any duplicates!")
            self.config['namespaceSet'] = ns_set

        if options.dest_ns_set:
            dest_ns_set = options.dest_ns_set.split(',')
            if len(dest_ns_set) != len(set(dest_ns_set)):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should not"
                    " contain any duplicates!")
            if not options.ns_set or len(dest_ns_set) != len(ns_set):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should be the"
                    " same length as the origin namespace set")
            mapping = dict(zip(ns_set, dest_ns_set))
            self.config['destMapping'] = mapping

        if options.fields:
            fields = options.fields.split(',')
            self.config['fields'] = fields

        if 'docManagers' in self.config:
            if options.target_urls or options.doc_managers:
                raise errors.InvalidConfiguration(
                    "Doc Managers settings in the configuration file"
                    " cannot be overwritten by command line arguments.")
        else:
            # Create doc managers from command line arguments
            self.config['docManagers'] = []

            target_urls = options.target_urls.split(",") \
                          if options.target_urls else []
            doc_managers = options.doc_managers.split(",") \
                           if options.doc_managers else []

            if len(target_urls) > 0 and len(doc_managers) == 0:
                raise errors.InvalidConfiguration(
                    "Cannot create a Connector with a target URL"
                    " but no doc manager!")

            # target_urls may be shorter than doc_managers
            for i, d in enumerate(doc_managers):
                doc_manager = {'docManager': d}
                if i < len(target_urls):
                    doc_manager['targetUrl'] = target_urls[i]
                else:
                    doc_manager['targetUrl'] = None
                self.config['docManagers'].append(doc_manager)

            # If more target URLS were given than doc managers, may
            # need to create additional doc managers
            for target_url in target_urls[len(doc_managers):]:
                doc_manager = {'docManager': doc_managers[-1]}
                doc_manager['targetUrl'] = target_url
                self.config['docManagers'].append(doc_manager)

    def validate(self):
        if self.config['syslog']['enabled'] and self.config['logFile']:
            raise errors.InvalidConfiguration(
                "You cannot specify syslog and a logfile simultaneously,"
                " please choose the logging method you would prefer.")

        defaultUsername = constants.DEFAULT_CONFIG['adminUsername']
        if self.config['adminUsername'] != defaultUsername:
            if self.config['password'] is None and \
               self.config['passwordFile'] is None:
                raise errors.InvalidConfiguration(
                    "Admin username specified without password!")

        if self.config['autoCommitInterval'] is not None:
            if self.config['autoCommitInterval'] < 0:
                raise errors.InvalidConfiguration(
                    "--auto-commit-interval must be non-negative")
"""
