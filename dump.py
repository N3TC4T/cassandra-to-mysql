# -*- coding: utf-8 -*-

import codecs
import sys
from time import time
import fileinput
from subprocess import call

try:
    import cassandra
    import cassandra.concurrent
except ImportError:
    sys.exit('Python Cassandra driver not installed. You might try \"pip install cassandra-driver\".')

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory

reload(sys)
sys.setdefaultencoding('utf-8')


# CONFIG SETS ====================================================================

# connection details
# if there is no auth please clear the value for USERNAME and PASSWORD
HOST = ['127.0.0.1'] # cassandra host to connect
USERNAME = 'user' # cassandra username
PASSWORD = 'password' # cassandra password
KEYSPACE = 'keyspace' # cassandra keyspace
TIMEOUT = 999999.0 # timeout set to max
# ----------------------------------------
INCLUDE_LIST = []
EXCLUDE_LIST = []
# ----------------------------------------
# if your table have primary key in uuid and you want to replace it with int
# define it here
PRIMARY_KEY = {
    "users": "user_id",
    "transactions": "tx_id",
}

# if there is a relation between your tables, and primary key is uuid this
# can be used to repleace uuid with int in each dump so it would sepratly store
# replaced UUID:INT in a file so it can be replaced in the dump file

# so for example UUID from table users will be store in a file with new int
# value and you can use replace.py file to replace the UUID with new int value
# in table transactions as user ids are stores in transaction tables
INCLUDE_UUID_REPLACEMENT_TABLES = ['table1']


# CONFIG SETS ====================================================================
def bar(total, current, desc):
    precent = current * 100 / total
    sys.stdout.write(('=' * precent) + ('' * (100 - precent)) + (
                "\r[ " + str(total) + " / " + str(current) + " ] [ %d" % precent + "% ] [ " + desc + " ] "))
    sys.stdout.flush()


def to_utf8(o):
    if isinstance(o, str):
        return codecs.decode(o, 'utf-8')
    else:
        return o

def replace_uuid(old_value, new_value, tablename):
    # save old value and new value for replacing foreignkey
    if tablename in INCLUDE_UUID_REPLACEMENT_TABLES :
        with open("output/%s-uuid-replacements.txt" % tablename, 'a+') as f:
            f.write("%s:%s\n" % (old_value, new_value))
    return new_value



def setup_cluster():
    cluster = None

    if USERNAME and PASSWORD:
        auth = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
        cluster = Cluster(contact_points=HOST, auth_provider=auth)
    else:
        cluster = Cluster(contact_points=HOST)

    session = cluster.connect()

    session.default_timeout = TIMEOUT
    session.row_factory = ordered_dict_factory
    return session


def cleanup_cluster(session):
    session.cluster.shutdown()
    session.shutdown()

def dump_all(session):
    keyspace = session.cluster.metadata.keyspaces.get(KEYSPACE)

    for tablename, tableval in keyspace.tables.iteritems():
        if not INCLUDE_LIST:
            if tablename in EXCLUDE_LIST:
                print ("Skipping data migrate for table %s") % tablename
                continue
        else:
            if tablename not in INCLUDE_LIST:
                print ("Skipping data migrate for table %s") % tablename
                continue

        print ("Exporting data for table %s") % tablename
        query = 'SELECT * FROM "' + KEYSPACE + '"."' + tablename + '"'

        encoded_rows = []
        auto_inc = 0

        columns = tableval.columns

        with open("output/%s.sql" % tablename, 'a+') as f:
            f.write('INSERT INTO %(tablename)s (%(columns)s) VALUES' % dict(
                tablename=to_utf8(tablename),
                columns=u', '.join(u'{}'.format(c) for c in columns),
            ))
        for row in session.execute(query):
            # change primary key uuid with auto increasment int
            auto_inc += 1
            values = []
            for k, v in row.iteritems():
                if PRIMARY_KEY[tablename] == k:
                    row[k] = replace_uuid(v, auto_inc, tablename)
                else:
                    row[k] = to_utf8(v)

            _temp = ''
            for i, (k, v) in enumerate(columns.iteritems()):
                if not isinstance(row[k], bool):
                    _temp += u"'{}'".format(to_utf8(row[k]))
                else:
                    _temp += u"{}".format(to_utf8(row[k]))

                if i != len(columns) - 1:
                    _temp += u", "

            values.append(u'({})'.format(to_utf8(_temp)))

            with open("output/%s.sql" % tablename, 'a+') as f:
                f.write('\n%(values)s,' % dict(
                    values=u',\n'.join(v for v in values),
                ))


if __name__ == '__main__':
    session = setup_cluster()
    dump_all(session)
    cleanup_cluster(session)

    print ("\n[!] EVERYTHING DONE!\n")
