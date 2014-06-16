import math
import StringIO
import logging
import pymongo
from mongo_connector import errors, util

# Helper for debugging
def get_file(main_connection, fs, filename):
    f = fs.find({'filename': filename})[0]
    return GridFSFile(main_connection, {
        '_id': f._id,
        'length': f.length,
        'uploadDate': f.upload_date,
        'md5': f.md5,
        'filename': f.filename,
        'chunkSize': f.chunk_size,
        'ns': 'test.fs.files',
        '_ts': 0,
    })

class GridFSFile:
    def __init__(self, main_connection, doc):
        self.main_connection = main_connection
        self._id = doc['_id']
        self._ts = doc['_ts']
        self.length = doc['length']
        self.chunk_size = doc['chunkSize']
        self.upload_date = doc['uploadDate']
        self.md5 = doc['md5']
        self.filename = doc.get('filename')

        # get the db and chunks collection from the namespace
        db, files_coll = doc['ns'].split(".", 1)
        fs, _ = files_coll.rsplit(".", 1)
        self.chunks_coll = fs + ".chunks"
        self.db = db
        self.ns = db + '.' + fs

        self.started_reading = False
        self.finished_reading = False
        self.header = ""
        self.footer = ""

        self.extra = ""
        self.cursor = self.main_connection[self.db][self.chunks_coll].find(
            {'files_id': self._id},
            sort=[('n', pymongo.ASCENDING)])

    def set_encoding(self, encoding):
        self.encoding = encoding

    def set_header(self, header):
        if self.started_reading:
            raise Exception("Can't set header after read() has been called.")
        self.header = header

    def set_footer(self, footer):
        if self.started_reading:
            raise Exception("Can't set footer after read() has been called.")
        self.footer = footer

    def __len__(self):
        return self.length
    
    def __repr__(self):
        return "GridFSFile(_id=%s, filename=%s, length=%d)" % (
            self._id, self.filename, self.length)

    def read(self, n = None):
        if n == None:
            n = self.length

        # Add header text before the file starts
        if not self.started_reading:
            self.extra = self.header
            self.started_reading = True

        data = [self.extra]
        loaded = len(self.extra)

        # Load at least n bytes
        while loaded < n:
            try:
                s = retry_until_ok(next, self.cursor)['data']
                if self.encoding != None:
                    s = s.encode(self.encoding)

                data.append(s)
                loaded += len(s)
                del s

            except StopIteration:
                # Add footer text after the file ends
                if not finished_reading:
                    data.append(self.footer)
                    finished_reading = True
                break

        # Keep the leftover bytes in self.extra
        data_str = "".join(data)
        result = data_str[:n]
        self.extra = data_str[n:]
        return result
