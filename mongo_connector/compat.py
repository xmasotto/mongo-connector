"""Utilities for maintaining portability between various Python versions"""

import sys

PY3 = (sys.version_info[0] == 3)

if PY3:
    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)

    def is_string(x):
        return isinstance(x, str)

else:
    exec("""def reraise(exctype, value, trace=None):
    raise exctype, str(value), trace
    """)

    def is_string(x):
        return isinstance(x, basestring)
