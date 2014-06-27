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

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.reset_config()

    def reset_config(self):
        self.options = get_config_options()
        self.conf = config.Config(self.options)

    def load_json(self, d, validate=True):
        # Serialize a python dictionary to json, then load it
        text = json.dumps(d)
        self.conf.load_json(text)
        if validate:
            self.load_options()

    def load_options(self, d={}):
        argv = []
        for k, v in d.items():
            argv.append(str(k))
            argv.append(str(v))
        self.conf.parse_args(argv)

    def test_default(self):
        # Make sure default configuration is valid
        self.load_options({})

    def test_parse_json(self):
        # Test for basic json parsing correctness
        test_config = {
            'mainAddress': 'testMainAddress',
            'oplogFile': 'testOplogFile',
            'noDump': True,
            'batchSize': 69,
            'verbosity': 3,
            'logging': {
                'type': 'file',
                'filename': 'testFilename',
                'host': 'testHost',
                'facility': 'testFacility'
            },
            'authentication': {
                'adminUsername': 'testAdminUsername',
                'password': 'testPassword',
                'passwordFile': 'testPasswordFile'
            },
            'fields': ['testFields1', 'testField2'],
            'namespaces': {
                'include': ['testNamespaceSet'],
                'map': {'testMapKey': 'testMapValue'}
            }
        }
        self.load_json(test_config, validate=False)

        test_keys = [k for k in test_config.keys() if k != "syslog"]
        for test_key in test_keys:
            self.assertEqual(self.conf[test_key], test_config[test_key])

    def test_basic_options(self):
        # Test the assignment of individual options
        def test_option(arg_name, json_key, value):
            self.load_options({arg_name : value})
            self.assertEqual(self.conf[json_key], value)
            self.reset_config()

        test_option('-m', 'mainAddress', 'testMainAddress')
        test_option('-o', 'oplogFile', 'testOplogFileShort')
        test_option('--batch-size', 'batchSize', 69)
        test_option('--continue-on-error', 'continueOnError', True)
        test_option('-v', 'verbosity', 1)

        self.load_options({'-w': 'logFile'})
        self.assertEqual(self.conf['logging.type'], 'file')
        self.assertEqual(self.conf['logging.filename'], 'logFile')

        self.load_options({'-s': 'true',
                      '--syslog-host': 'testHost',
                      '--syslog-facility': 'testFacility'})
        self.assertEqual(self.conf['logging.type'], 'syslog')
        self.assertEqual(self.conf['logging.host'], 'testHost')
        self.assertEqual(self.conf['logging.facility'], 'testFacility')

        self.load_options({'-i': 'a,b,c'})
        self.assertEqual(self.conf['fields'], ['a', 'b', 'c'])

    def test_namespace_set(self):
        # test namespace_set and dest_namespace_set
        self.load_options({
            "-n": "source_ns_1,source_ns_2,source_ns_3",
            "-g": "dest_ns_1,dest_ns_2,dest_ns_3"
        })
        self.assertEqual(self.conf['namespaces.include'],
                          ['source_ns_1', 'source_ns_2', 'source_ns_3'])
        self.assertEqual(self.conf['namespaces.mapping'],
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
        self.assertEqual(self.conf['docManagers'], docManagers);

        self.reset_config()

        # no doc_manager but target_urls
        args = {
            "-t": "1,2"
        }
        self.assertRaises(errors.InvalidConfiguration,
                          self.load_options, args)

        self.reset_config()

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
        self.assertEqual(self.conf['docManagers'], docManagers);

        self.reset_config()

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
        self.assertEqual(self.conf['docManagers'], docManagers);

        # don't reset doc managers

        # Test that args can't overwrite docManager configurations
        args = {
            '-d': 'a',
            '-t': '1'
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

    def test_config_validation(self):
        # can't log both to syslog and to logfile
        self.assertRaises(errors.InvalidConfiguration,
                          self.load_options, {
                              '-w': 'logFile', '-s': 'true'
                          })

        # can't specify a username without a password
        self.assertRaises(errors.InvalidConfiguration,
                          self.load_options, {
                              '-a': 'username'
                          })

        self.reset_config()

        # can't specify password and password file
        self.assertRaises(errors.InvalidConfiguration,
                          self.load_options, {
                              '-a': 'username',
                              '-p': 'password',
                              '-f': 'password_file'
                          })

        self.reset_config()

        # docManagers must be a list
        test_config = {
            'docManagers': "hello"
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)

        # every element of docManagers must contain a 'docManager' property
        test_config = {
            'docManagers': [
                {
                    'targetURL': 'testTargetURL'
                }
            ]
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)

        # auto commit interval can't be negative
        test_config = {
            'docManagers': [
                {
                    'docManager': 'testDocManager',
                    'autoCommitInterval': -1
                }
            ]
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)
