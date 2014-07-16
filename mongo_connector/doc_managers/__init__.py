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
import logging
import sys

from mongo_connector.compat import reraise
from mongo_connector.errors import OperationFailed, UpdateDoesNotApply


def exception_wrapper(mapping):
    def decorator(f):
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                exc_type, exc_value, exc_tb = sys.exc_info()
                new_type = mapping.get(exc_type)
                if new_type is None:
                    raise
                reraise(new_type, exc_value, exc_tb)
        return wrapped
    return decorator


class DocManagerBase(object):
    """Base class for all DocManager implementations."""

    def apply_update(self, doc, update_spec):
        """Apply an update operation to a document."""

        # Helper to cast a key for a list or dict, or raise ValueError
        def _convert_or_raise(container, key):
            if isinstance(container, dict):
                return key
            elif isinstance(container, list):
                return int(key)
            else:
                raise ValueError

        # Helper to retrieve (and/or create)
        # a dot-separated path within a document.
        def _retrieve_path(container, path, create=False):
            looking_at = container
            for part in path:
                if isinstance(looking_at, dict):
                    if create and not part in looking_at:
                        looking_at[part] = {}
                    looking_at = looking_at[part]
                elif isinstance(looking_at, list):
                    index = int(part)
                    if create and len(looking_at) < index:
                        looking_at.extend(
                            [None] * (len(looking_at) - index - 1))
                        looking_at.append({})
                    looking_at = looking_at[index]
                else:
                    raise ValueError
            return looking_at

        # wholesale document replacement
        if not "$set" in update_spec and not "$unset" in update_spec:
            # update spec contains the new document in its entirety
            update_spec['_ts'] = doc['_ts']
            update_spec['ns'] = doc['ns']
            return update_spec
        else:
            try:
                # $set
                for to_set in update_spec.get("$set", []):
                    value = update_spec['$set'][to_set]
                    if '.' in to_set:
                        path = to_set.split(".")
                        where = _retrieve_path(doc, path[:-1], create=True)
                        where[_convert_or_raise(where, path[-1])] = value
                    else:
                        doc[to_set] = value

                # $unset
                for to_unset in update_spec.get("$unset", []):
                    if '.' in to_unset:
                        path = to_unset.split(".")
                        where = _retrieve_path(doc, path[:-1])
                        where.pop(_convert_or_raise(where, path[-1]))
                    else:
                        doc.pop(to_unset)
            except (KeyError, ValueError, AttributeError, IndexError):
                exc_t, exc_v, exc_tb = sys.exc_info()
                reraise(UpdateDoesNotApply,
                        "Cannot apply update %r to %r" % (update_spec, doc),
                        exc_tb)
            return doc

    def handle_command(self, doc):
        db, coll = doc['ns'].split('.', 1)
        if coll != "$cmd":
            raise OperationFailed("Invalid Oplog Command")

        if db == 'admin':
            if doc.get('renameCollection'):
                if doc['dropTarget']:
                    self.drop_collection(doc['to'])
                self.rename_collection(
                    doc['renameCollection'],
                    doc['to'])
        else:
            if doc.get('dropDatabase'):
                self.drop_database(db)

            if doc.get('create'):
                self.create_collection(db + '.' + doc['create'])

            if doc.get('drop'):
                self.drop_collection(db + '.' + doc['drop'])

    def bulk_upsert(self, docs):
        """Upsert each document in a set of documents.

        This method may be overridden to upsert many documents at once.
        """
        for doc in docs:
            self.upsert(doc)

    def update(self, doc, update_spec):
        """Update a document.

        ``update_spec`` is the update operation as provided by an oplog record
        in the "o" field.
        """
        raise NotImplementedError

    def upsert(self, document):
        """(Re-)insert a document."""
        raise NotImplementedError

    def remove(self, doc):
        """Remove a document.

        ``doc`` is a dict that provides the namespace and id of the document
        to be removed in its ``ns`` and ``_id`` fields, respectively.
        """
        raise NotImplementedError

    def search(self, start_ts, end_ts):
        """Get an iterable of documents that were inserted or updated
        between ``start_ts`` and ``end_ts``.
        """
        raise NotImplementedError

    def commit(self):
        """Commit all outstanding writes."""
        raise NotImplementedError

    def get_last_doc(self):
        """Get the document that was modified most recently."""
        raise NotImplementedError

    def stop(self):
        """Stop all threads started by this DocManager."""
        raise NotImplementedError

    def create_collection(self, ns):
        """Explicitly create a collection with the given namespace."""
        print("create_collection " + ns)
        logging.warning("%r does not support replication of the"
                        " 'create_collection' command." % type(self).__name__)

    def rename_collection(self, old_ns, new_ns):
        """Rename a collection."""
        print("rename_collection " + old_ns + " -> " + new_ns)
        logging.warning("%r does not support replication of the"
                        " 'rename_collection' command." % type(self).__name__)

    def drop_collection(self, ns):
        """Drop a collection"""
        print("drop_collection " + ns)
        logging.warning("%r does not support replication of the"
                        " 'drop_collection' command." % type(self).__name__)

    def drop_database(self, db):
        """Drop a database"""
        print("drop_database " + ns)
        logging.warning("%r does not support replication of the"
                        " 'drop_database' command." % type(self).__name__)
