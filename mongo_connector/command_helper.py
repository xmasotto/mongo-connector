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

"""Preprocesses the oplog command entries.
"""

import logging

LOG = logging.getLogger(__name__)


class CommandHelper:
    def __init__(self, namespace_set, dest_mapping):
        self.namespace_set = namespace_set
        self.dest_mapping = dest_mapping

        # Create a db to db mapping from the namespace mapping.
        db_pairs = [(ns.split('.')[0],
                     self.map_namespace(ns).split('.')[0])
                    for ns in self.namespace_set]
        mapping = dict(db_pairs)

        # Database commands can only be replicated if the
        # database mapping is bijective.
        surjective = len(mapping) == len(db_pairs)
        injective = len(set(mapping.values())) == len(mapping)
        self.db_bijection = surjective and injective
        self.db_mapping = mapping

    def nullify(self, doc):
        for k in list(doc):
            del doc[k]

    def map_namespace(self, ns):
        if not self.namespace_set:
            return ns
        elif ns not in self.namespace_set:
            return None
        else:
            return self.dest_mapping.get(ns, ns)

    def map_db(self, db):
        if not self.db_mapping:
            return db
        elif db not in self.db_mapping:
            return None
        else:
            return self.db_mapping.get(db, db)

    def rewrite_db(self, doc, command_name):
        if doc.get(command_name):
            if not self.db_bijection:
                raise OperationFailed(
                    "Cannot rewrite database for %s command:"
                    "Database mapping is not bijective.")

            db = self.map_db(doc['db'])
            if db:
                doc['db'] = db
            else:
                LOG.warning(
                    "Skipping replication of %s command since"
                    " %s isn't in the namespace set." %
                    (command_name, doc['db']))
                self.nullify(doc)

    def rewrite_collection(self, doc, command_name, key):
        if doc.get(command_name):
            ns = self.map_namespace(doc['db'] + '.' + doc[key])
            if ns:
                doc['db'], doc[key] = ns.split('.', 1)
            else:
                LOG.warning(
                    "Skipping replication of %s command since"
                    " %s isn't in namespace set."
                    % (command_name, doc['db'] + '.' + doc[key]))
                self.nullify(doc)

    def rewrite_namespace(self, doc, command_name, key):
        if doc.get(command_name):
            ns = self.map_namespace(doc.get(key))
            if ns:
                doc[key] = ns
            else:
                LOG.warning(
                    "Skipping replication of %s command since"
                    " %s isn't in namespace set."
                    % (command_name, doc[key]))
                self.nullify(doc)
