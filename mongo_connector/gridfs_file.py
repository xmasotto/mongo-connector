import math
import StringIO
import logging
import pymongo
from mongo_connector import errors, util

def get_file(db, fs, filename):
    f = fs.find({'filename': filename})[0]
    return GridFSFile(db, {
        '_id': f._id,
        'length': f.length,
        'uploadDate': f.upload_date,
        'md5': f.md5,
        'filename': f.filename,
        'chunkSize': f.chunk_size,
        'ns': 'test.fs.files'
    })

class GridFSFile:
    def __init__(self, main_connection, doc):
        self.main_connection = main_connection
        self._id = doc.get("_id")
        self.length = doc['length']
        self.uploadDate = doc['uploadDate']
        self.md5 = doc['md5']
        self.filename = doc.get('filename') #optional

        # get the db and chunks collection from the namespace
        db, files_coll = doc['ns'].split(".", 1)
        fs, _ = files_coll.rsplit(".", 1)
        self.chunks_coll = fs + ".chunks"
        self.db = db

        self.extra = ""
        self.cursor = self.main_connection[self.db][self.chunks_coll].find(
            {'files_id': self._id},
            sort=[('n', 1)]
        )

    def __len__(self):
        return self.length

    def read(self, n = None):
        if n == None:
            n = self.length

        f = StringIO.StringIO(self.extra)
        loaded = len(self.extra)

        while loaded < n:
            try:
                print("Loading Chunk")
                s = util.retry_until_ok(next, self.cursor)['data']
                f.write(s)
                loaded += len(s)
                del s
            except StopIteration:
                break

        f.seek(0)
        result = f.read(n)
        self.extra = f.read()
        f.close()
        return result
