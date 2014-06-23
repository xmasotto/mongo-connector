import gridfs
import logging
import math
import pymongo
import time

from mongo_connector import compat, errors, util
from mongo_connector.doc_managers import exception_wrapper

wrap_exceptions = exception_wrapper({
    gridfs.errors.CorruptGridFile : errors.OperationFailed,
    gridfs.errors.NoFile : errors.OperationFailed
})

class GridFSFile(object):
    @wrap_exceptions
    def __init__(self, main_connection, doc):
        self._id = doc['_id']
        self._ts = doc['_ts']
        self.ns = doc['ns']

        db, coll = self.ns.split(".", 1)
        self.fs = gridfs.GridFS(main_connection[db], coll)

        try:
            self.f = next(self.fs.find({'_id': self._id}))
            self.filename = self.f.filename
            self.length = self.f.length
            self.upload_date = self.f.upload_date
            self.md5 = self.f.md5
        except StopIteration:
            self.f = None

        self.ensure_file()

    def ensure_file(self):
        if self.f is None:
            raise errors.OperationFailed(
                "GridFS file with %r does not exist!" % self._id)

    @wrap_exceptions
    def __len__(self):
        self.ensure_file()
        return self.length

    @wrap_exceptions
    def read(self, n=-1):
        self.ensure_file()
        return self.f.read(n)
