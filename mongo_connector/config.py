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
import logging
import optparse
import sys

from mongo_connector import compat, constants, errors
from mongo_connector.compat import reraise

def default_apply_function(option, cli_values):
    first_value = list(cli_values.values())[0]
    if first_value is not None:
        option.value = first_value

class Option(object):
    def __init__(self, config_key=None, default=None,
                 apply_function=default_apply_function):
        self.config_key = config_key
        self.value = default
        self.apply_function = apply_function

        self.cli_names = []
        self.cli_options = []
        self.option_type = None
        self.validate_type = lambda x:True

    def set_type(self, option_type):
        self.option_type = option_type
        if option_type == str:
            self.validate_type = compat.is_string
        else:
            self.validate_type = lambda x:isinstance(x,option_type)

    def add_cli(self, *args, **kwargs):
        self.cli_options.append( (args, kwargs) )

class Config(object):
    def __init__(self, options):
        self.options = options

        self.config_key_to_option = dict(
            [(option.config_key, option) for option in self.options])

    def parse_args(self, argv=None):
        # parse the command line options
        parser = optparse.OptionParser()
        for option in self.options:
            for args, kwargs in option.cli_options:
                cli_option = parser.add_option(*args, **kwargs)
                option.cli_names.append(cli_option.dest)
        parsed_options, args = parser.parse_args(argv)

        # load the config file
        if parsed_options.config_file:
            try:
                with open(parsed_options.config_file) as f:
                    self.load_json(f.read())
            except (OSError, IOError, ValueError):
                reraise(errors.InvalidConfiguration, *sys.exc_info()[1:])

        # apply the command line arguments
        values = parsed_options.__dict__
        for option in self.options:
            option.apply_function(
                option, dict((k, values.get(k)) for k in option.cli_names))

    def __getitem__(self, key):
        keys = key.split('.')
        cur = self.config_key_to_option[keys[0]].value
        for k in keys[1:]:
            if cur is not None:
                if isinstance(cur, dict):
                    cur = cur.get(k)
                else:
                    cur = None
        return cur

    def load_json(self, text):
        parsed_config = json.loads(text)
        for k in parsed_config:
            option = self.config_key_to_option.get(k)
            if option:
                # load into option.value
                if isinstance(parsed_config[k], dict):
                    for k2 in parsed_config[k]:
                        option.value[k2] = parsed_config[k][k2]
                else:
                    option.value = parsed_config[k]

                # type check
                if not option.validate_type(option.value):
                    raise errors.InvalidConfiguration(
                        "%s should have %r, %r was given!" %
                        (option.config_key,
                         option.option_type,
                         type(option.value)))
            else:
                if not k.startswith("__"):
                    logging.warning("Unrecognized option: %s" % k)
