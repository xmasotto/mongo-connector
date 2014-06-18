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

import sys

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import gridfs

from tests import mongo_host
from pymongo import MongoClient
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector import errors
from tests.setup_cluster import (
    start_replica_set,
    kill_replica_set
)

sys.path[0:0] = [""]

from mongo_connector.gridfs_file import GridFSFile

class TestGridFSFile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Start up a replica set and connect to it
        _, _, cls.primary_p = start_replica_set('test-gridfs-file')
        cls.main_connection = MongoClient("%s:%d" % (mongo_host, cls.primary_p))
        
    @classmethod
    def tearDownClass(cls):
        cls.main_connection.close()
        kill_replica_set('test-gridfs-file')

    def setUp(self):
        # clear existing data
        self.main_connection.drop_database("test")
        self.fs = gridfs.GridFS(self.main_connection['test'])

    def get_file(self, id):
        doc = {
            '_id': id,
            'ns': 'test.fs.files',
            '_ts': 0
        }
        return GridFSFile(self.main_connection, doc)

    def test_insert(self):
        def test_insert_file(data, filename, read_size):
            # insert file
            id = self.fs.put(data, filename=filename)
            f = self.get_file(id)

            # test metadata
            self.assertEquals(id, f._id)
            self.assertEquals(filename, f.filename)

            # test data
            result = []
            while True:
                s = f.read(read_size)
                if len(s) > 0:
                    result.append(s)
                    if read_size >= 0:
                        self.assertLessEqual(len(s), read_size)
                else:
                    break
            result = "".join(result)
            self.assertEquals(f.length, len(result))
            self.assertEquals(data, result)

        # test with 1-chunk files
        test_insert_file("hello world", "hello.txt", -1)
        test_insert_file("hello world 2", "hello.txt", 10)
        test_insert_file("hello world 3", "hello.txt", 100)

        # test with multiple-chunk files
        size = 4 * 1024 * 1024
        bigger = "".join([chr(ord('a') + (n % 26)) for n in range(size)])
        test_insert_file(bigger, "bigger.txt", -1)
        test_insert_file(bigger, "bigger.txt", 1024)
        test_insert_file(bigger, "bigger.txt", 1024 * 1024)

    def test_missing_file(self):
        data = "test data"
        id = self.fs.put(data)
        self.fs.delete(id)
        self.assertRaises(errors.OperationFailed, self.get_file, id)

    def test_corrupted_file(self):
        data = "test data"
        id = self.fs.put(data)
        f = self.get_file(id)

        result = self.main_connection['test']['fs.chunks'].remove({
            'files_id': id
        })

        self.assertRaises(errors.OperationFailed, f.read)
