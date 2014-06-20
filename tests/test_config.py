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
from mongo_connector.connector import get_config_options
from mongo_connector.connector import validate_config

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.reset_config()
        self.options_by_name = {}
        for option in self.options:
            for name in option.names:
                self.options_by_name[name] = option

    def reset_config(self):
        self.options = get_config_options()
        self.conf = config.Config(self.options)

    def load_json(self, d):
        # Serialize a python dictionary to json, then load it
        text = json.dumps(d)
        self.conf.load_json(text)

    def load_options(self, d):
        values = {}
        for name, value in d.items():
            values[self.options_by_name[name].dest] = value

        for name, value in d.items():
            if value:
                self.options_by_name[name].apply(values)

    def test_default(self):
        # Make sure default configuration is valid
        validate_config(self.conf)

    def test_parse_json(self):
        # Test for basic json parsing correctness
        test_config = {
            'mainAddress': 'testMainAddress',
            'oplogFile': 'testOplogFile',
            'noDump': True,
            'batchSize': 69,
            'passwordFile': 'testPasswordFile',
            'password': 'testPassword',
            'adminUsername': 'testAdminUsername',
            'continueOnError': True,
            'verbosity': 1,

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

        test_keys = [k for k in test_config.keys() if k != "syslog"]
        for test_key in test_keys:
            self.assertEquals(self.conf[test_key], test_config[test_key])

        self.assertEquals(self.conf['syslog.enabled'], 
                          test_config['syslog']['enabled'])
        self.assertEquals(self.conf['syslog.host'], 
                          test_config['syslog']['host'])

    def test_basic_options(self):
        # Test the assignment of individual options
        def test_option(arg_name, json_key, value):
            self.load_options({arg_name : value})
            self.assertEquals(self.conf[json_key], value)

        test_option('-m', 'mainAddress', 'testMainAddressShort')
        test_option('--main', 'mainAddress', 'testMainAddressLong')
        test_option('-o', 'oplogFile', 'testOplogFileShort')
        test_option('--oplog-ts', 'oplogFile', 'testOplogFileLong')
        test_option('--batch-size', 'batchSize', 69)
        test_option('-f', 'passwordFile', 'testPasswordFileShort')
        test_option('--password-file', 'passwordFile', 'testPasswordFileLong')
        test_option('-p', 'password', 'testPasswordShort')
        test_option('--password', 'password', 'testPasswordLong')
        test_option('-a', 'adminUsername', 'testAdminUsername1')
        test_option('--admin-username', 'adminUsername', 'testAdminUsername2')
        test_option('--continue-on-error', 'continueOnError', True)
        test_option('-w', 'logFile', 'testLogFileShort')
        test_option('--logfile', 'logFile', 'testLogFileLong')
        test_option('--syslog-host', 'syslog.host', "testSyslogHost")
        test_option('--syslog-facility', 'syslog.facility', "testSyslogFaciliy")

        test_option('-v', 'verbosity', 1)
        self.conf.get_option('verbosity').value = 0
        test_option('--verbose', 'verbosity', 1)

        test_option('-s', 'syslog.enabled', True)
        self.conf.get_option('syslog.enabled').value = False
        test_option('--enable-syslog', 'syslog.enabled', False)

        self.load_options({'-i': 'a,b,c'})
        self.assertEquals(self.conf['fields'], ['a', 'b', 'c'])

        self.load_options({'--fields': 'd,e'})
        self.assertEquals(self.conf['fields'], ['d', 'e'])

    def test_namespace_set(self):
        # test namespace_set and dest_namespace_set
        self.load_options({
            "-n": "source_ns_1,source_ns_2,source_ns_3",
            "-g": "dest_ns_1,dest_ns_2,dest_ns_3"
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
            "-n": "a,a,b",
            "-g": "1,2,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

        # duplicate dest_ns_set
        args = {
            "-n": "a,b,c",
            "--dest-namespace-set": "1,3,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

        # len(ns_set) < len(dest_ns_set)
        args = {
            "--namespace-set": "a,b,c",
            "-g": "1,2,3,4"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

        # len(ns_set) > len(dest_ns_set)
        args = {
            "--namespace-set": "a,b,c,d",
            "--dest-namespace-set": "1,2,3"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

    def test_doc_managers_from_args(self):
        # Test basic docmanager construction from args
        args = {
            "-d": "a,b",
            "-t": "1,2",
            '-u': "id",
            '--auto-commit-interval': 10
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetURL': '1',
                'uniqueKey': 'id',
                'autoCommitInterval': 10
            },
            {
                'docManager': 'b',
                'targetURL': '2',
                'uniqueKey': 'id',
                'autoCommitInterval': 10
            }
        ]
        self.load_options(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        self.conf.get_option('docManagers').value = None # reset doc managers

        # no doc_manager but target_urls
        args = {
            "-d": None,
            "-t": "1,2"
        }
        self.assertRaises(errors.InvalidConfiguration,
                          self.load_options, args)

        # fewer doc_managers than target_urls
        args = {
            "--docManager": "a",
            "--target-url": "1,2"
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetURL': '1',
                'uniqueKey': constants.DEFAULT_UNIQUE_KEY,
                'autoCommitInterval': constants.DEFAULT_COMMIT_INTERVAL
            },
            {
                'docManager': 'a',
                'targetURL': '2',
                'uniqueKey': constants.DEFAULT_UNIQUE_KEY,
                'autoCommitInterval': constants.DEFAULT_COMMIT_INTERVAL
            }
        ]
        self.load_options(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        self.conf.get_option('docManagers').value = None # reset doc managers

        # fewer target_urls than doc_managers
        args = {
            "--doc-managers": "a,b",
            "--target-urls": "1"
        }
        docManagers = [
            {
                'docManager': 'a',
                'targetURL': '1',
                'uniqueKey': constants.DEFAULT_UNIQUE_KEY,
                'autoCommitInterval': constants.DEFAULT_COMMIT_INTERVAL
            },
            {
                'docManager': 'b',
                'targetURL': None,
                'uniqueKey': constants.DEFAULT_UNIQUE_KEY,
                'autoCommitInterval': constants.DEFAULT_COMMIT_INTERVAL
            }
        ]
        self.load_options(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        # don't reset doc managers

        # Test that args can't overwrite docManager configurations
        args = {
            '-d': 'a',
            '-t': '1'
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

    def test_config_validation(self):
        # can't log both to syslog and to logfile
        test_config = {
            'logFile': 'testLogFile',
            'syslog': {
                'enabled': True
            }
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)

        self.reset_config()

        # can't specify a username without a password
        test_config = {
            'adminUsername': 'testUsername'
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)

        self.reset_config()

        # docManagers must be a list
        test_config = {
            'docManagers': "hello"
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)

        # every element of docManagers must contain a 'docManager' property
        test_config = {
            'docManagers': [
                {
                    'targetURL': 'testTargetURL'
                }
            ]
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)

        # auto commit interval can't be negative
        test_config = {
            'docManagers': [
                {
                    'docManager': 'testDocManager',
                    'autoCommitInterval': -1
                }
            ]
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)
