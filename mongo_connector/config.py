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

from mongo_connector import constants, errors
from mongo_connector.compat import reraise

def default_apply_function(option, values):
    option.value = values[option.dest]

class Option(object):
    def __init__(self, names=None, dest=None, config_key=None,
                 default=None, type=None, action="store", help=None,
                 apply_function=default_apply_function):
        self.names = names
        self.dest = dest
        self.config_key = config_key
        self.default = default
        self.type = type
        self.action = action
        self.help = help
        self.apply_function = apply_function
        self.value = self.default
    
    def apply(self, values):
        self.apply_function(self, values)

class Config(object):
    def __init__(self, options):
        self.options = options

        self.config_key_to_option = dict(
            [(option.config_key, option) for option in self.options])

    def parse_args(self):
        # parse the command line options
        parser = optparse.OptionParser()
        for option in self.options:
            if option.names:
                parser.add_option(*option.names, 
                                  dest=option.dest,
                                  action=option.action,
                                  type=option.type,
                                  help=option.help)

        parsed_options, args = parser.parse_args()

        # load the config file
        if parsed_options.config_file:
            try:
                with open(parsed_options.config_file) as f:
                    self.load_json(f.read())
            except (OSError, IOError, ValueError) as e:
                reraise(errors.InvalidConfiguration, *sys.exc_info()[1:])

        # apply the command line arguments
        values = parsed_options.__dict__
        for option in self.options:
            if values[option.dest]:
                option.apply(values)

    def get_option(self, key):
        return self.config_key_to_option[key]

    def __getitem__(self, key):
        return self.config_key_to_option[key].value

    def load_json(self, text):
        parsed_config = json.loads(text)
        self.unpack(parsed_config, "")

    def unpack(self, obj, prefix):
        for k in obj:
            option = self.config_key_to_option.get(prefix + k, None)
            if option:
                option.value = obj[k]

            elif type(obj[k]) is dict:
                self.unpack(obj[k], prefix + k + '.')
                
            else:
                if not prefix.startswith("__") and not k.startswith("__"):
                    logging.warning("Unrecognized option: %s" % (prefix + k))
