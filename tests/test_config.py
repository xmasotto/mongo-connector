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
from mongo_connector.connector import command_line_options, validate_config

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
        self.reset_config()
        self.options_by_name = {}
        for option in command_line_options:
            for name in option.names:
                self.options_by_name[name] = option

    def reset_config(self):
        self.conf = config.Config(command_line_options)

    def load_json(self, d):
        # Serialize a python dictionary to json, then load it
        text = json.dumps(d)
        self.conf.load_json(text)

    def load_options(self, d):
        values = {}
        for name, value in d.items():
            values[self.options_by_name[name].dest] = value

        for name, value in d.items():
            self.options_by_name[name].apply(self.conf.config, values)

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
            self.load_options({arg_name : value})
            self.assertEquals(self.conf[json_key], value)

        test_option('-m', 'mainAddress', 'testMainAddressShort')
        test_option('--main', 'mainAddress', 'testMainAddressLong')
        test_option('-o', 'oplogFile', 'testOplogFileShort')
        test_option('--oplog-ts', 'oplogFile', 'testOplogFileLong')
        test_option('--batch-size', 'batchSize', 69)
        test_option('-u', 'uniqueKey', 'testUniqueKeyShort')
        test_option('--unique-key', 'uniqueKey', 'testUniqueKeyLong')
        test_option('-f', 'passwordFile', 'testPasswordFileShort')
        test_option('--password-file', 'passwordFile', 'testPasswordFileLong')
        test_option('-p', 'password', 'testPasswordShort')
        test_option('--password', 'password', 'testPasswordLong')
        test_option('-a', 'adminUsername', 'testAdminUsernameShort')
        test_option('--admin-username', 'adminUsername', 'testAdminUsernameLong')
        test_option('--auto-commit-interval', 'autoCommitInterval', 69)
        test_option('--continue-on-error', 'continueOnError', True)
        test_option('-v', 'verbose', True)
        test_option('--verbose', 'verbose', False)
        test_option('-w', 'logFile', 'testLogFileShort')
        test_option('--logfile', 'logFile', 'testLogFileLong')

        def test_syslog_option(arg_name, json_key, value):
            self.load_options({arg_name : value})
            self.assertEquals(self.conf['syslog'][json_key], value)

        test_syslog_option('-s', 'enabled', True)
        test_syslog_option('--enable-syslog', 'enabled', False)
        test_syslog_option('--syslog-host', 'host', 'testHost')
        test_syslog_option('--syslog-facility', 'facility', 'testFacility')

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
            "-t": "1,2"
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
        self.load_options(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        del self.conf.config['docManagers'] # reset doc managers

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
                'targetUrl': '1'
            },
            {
                'docManager': 'a',
                'targetUrl': '2'
            }
        ]
        self.load_options(args)
        self.assertEquals(self.conf['docManagers'], docManagers);

        del self.conf.config['docManagers'] # reset doc managers

        # fewer target_urls than doc_managers
        args = {
            "--doc-managers": "a,b",
            "--target-urls": "1"
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

        # auto commit interval can't be negative
        test_config = {
            'autoCommitInterval': -1
        }
        self.load_json(test_config)
        self.assertRaises(errors.InvalidConfiguration, 
                          validate_config, self.conf)
