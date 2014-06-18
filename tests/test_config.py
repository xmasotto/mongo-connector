# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

from mongo_connector import config, constants, errors

class MockOptions(object):
    def __init__(self, options):
        self.options = options
    def __getattr__(self, key):
        if key in self.options:
            return self.options[key]
        else:
            return None

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.conf = config.Config()

    def load_json(self, d):
        # Serialize a python dictionary to json, then load it
        text = json.dumps(d)
        self.conf.add_config_json(text)

    def load_args(self, d):
        # Load options from a python dictionary
        options = MockOptions(d)
        self.conf.add_options(options)

    def test_default(self):
        # Make sure default configuration is valid
        try:
            self.conf.validate()
        except Exception as e:
            self.assertFalse("Default configuration invalid: %s" % e.message)

    def test_parse_json(self):
        # Test for basic json parsing correctness
        test_config = {
            'mainAddress': 'testMainAddress',
            'oplogFile': 'testOplogFile',
            'noDump': True,
            'batchSize': 69,
            'uniqueKey': 'testUniqueKey',
            'passwordFile': 'testPasswordFile',
            'password': 'testPassword',
            'adminUsername': 'testAdminUsername',
            'autoCommitInterval': 69,
            'continueOnError': True,
            'verbose': True,

            'logFile': "testLogFile",
            'syslog': {
                'enabled': False,
                'host': 'testSyslogHost',
                'facility': 'testSyslogFacility'
            },

            'namespaceSet': ['testNamespaceSet'],
            'destMapping': {'testMapKey': 'testMapValue'},
            'fields': ['testField1', 'testField2']
        }
        self.load_json(test_config)
        self.assertEquals(test_config, self.conf.config)

        # Test for correct nested config merging
        test_config = {
            'syslog': {
                'enabled': True
            }
        }
        self.load_json(test_config)
        self.assertEquals(self.conf['syslog']['enabled'], True)
        self.assertEquals(self.conf['syslog']['host'], "testSyslogHost")
        self.assertEquals(self.conf['syslog']['facility'], "testSyslogFacility")

    def test_basic_options(self):
        default = constants.DEFAULT_CONFIG

        # Test the assignment of individual options
        def test_option(arg_name, json_key, value):
            self.assertEquals(self.conf[json_key], default[json_key])
            self.load_args({arg_name : value})
            self.assertEquals(self.conf[json_key], value)

        test_option('main_address', 'mainAddress', 'testMainAddress')
        test_option('oplog_file', 'oplogFile', 'testOplogFile')
        test_option('batch_size', 'batchSize', 69)
        test_option('unique_key', 'uniqueKey', 'testUniqueKey')
        test_option('password_file', 'passwordFile', 'testPasswordFile')
        test_option('password', 'password', 'testPassword')
        test_option('admin_username', 'adminUsername', 'testAdminUsername')
        test_option('auto_commit_interval', 'autoCommitInterval', 69)
        test_option('continue_on_error', 'continueOnError', True)
        test_option('verbose', 'verbose', True)
        test_option('logfile', 'logFile', 'testLogFile')

        def test_syslog_option(arg_name, json_key, value):
            self.assertEquals(self.conf['syslog'][json_key],
                              default['syslog'][json_key])
            self.load_args({arg_name : value})
            self.assertEquals(self.conf['syslog'][json_key], value)

        test_syslog_option('enable_syslog', 'enabled', True)
        test_syslog_option('syslog_host', 'host', 'testHost')
        test_syslog_option('syslog_facility', 'facility', 'testFacility')

        self.assertEquals(self.conf['fields'], default['fields'])
        self.load_args({'fields': 'a,b,c'})
        self.assertEquals(self.conf['fields'], ['a', 'b', 'c'])

    def test_namespace_set(self):
        # test namespace_set and dest_namespace_set
        self.load_args({
            "ns_set": "source_ns_1,source_ns_2,source_ns_3",
            "dest_ns_set": "dest_ns_1,dest_ns_2,dest_ns_3"
        })
        self.assertEquals(self.conf['namespaceSet'],
                          ['source_ns_1', 'source_ns_2', 'source_ns_3'])
        self.assertEquals(self.conf['destMapping'],
                          {'source_ns_1': 'dest_ns_1',
                           'source_ns_2': 'dest_ns_2',
                           'source_ns_3': 'dest_ns_3'})

    def test_namespace_set_validation(self):
        # duplicate ns_set
        args = {
            "ns_set": "a,a,b",
            "dest_ns_set": "1,2,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_args, args)

        # duplicate dest_ns_set
        args = {
            "ns_set": "a,b,c",
            "dest_ns_set": "1,3,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_args, args)

        # len(ns_set) < len(dest_ns_set)
        args = {
            "ns_set": "a,b,c",
            "dest_ns_set": "1,2,3,4"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_args, args)

        # len(ns_set) > len(dest_ns_set)
        args = {
            "ns_set": "a,b,c,d",
            "dest_ns_set": "1,2,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_args, args)

    def test_doc_managers_from_args(self):
        # Test basic docmanager construction from args
        args = {
            "doc_managers": "a,b",
            "target_urls": "1,2"
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetUrl': '1'
            },
            {
                'docManager': 'b',
                'targetUrl': '2'
            }
        ]
        self.load_args(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        del self.conf.config['docManagers'] # reset doc managers

        # fewer doc_managers than target_urls
        args = {
            "doc_managers": "a",
            "target_urls": "1,2"
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetUrl': '1'
            },
            {
                'docManager': 'a',
                'targetUrl': '2'
            }
        ]
        self.load_args(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        del self.conf.config['docManagers'] # reset doc managers

        # fewer target_urls than doc_managers
        args = {
            "doc_managers": "a,b",
            "target_urls": "1"
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetUrl': '1'
            },
            {
                'docManager': 'b',
                'targetUrl': None
            }
        ]
        self.load_args(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        # don't reset doc managers

        # Test that args can't overwrite docManager configurations
        args = {
            'doc_managers': 'a',
            'target_urls': '1'
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_args, args)

    def test_config_validation(self):
        # can't log both to syslog and to logfile
        test_config = {
            'logFile': 'testLogFile',
            'syslog': {
                'enabled': True
            }
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, self.conf.validate)

        self.conf = config.Config() # reset configuration

        # can't specify a username without a password
        test_config = {
            'adminUsername': 'testUsername'
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, self.conf.validate)

        self.conf = config.Config() # reset configuration

        # auto commit interval can't be negative
        test_config = {
            'autoCommitInterval': -1
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, self.conf.validate)
