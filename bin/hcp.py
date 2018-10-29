# pylint: disable=C0301,C0111,W0212,W0611,E1102,W0703
import ssl
import sys
import json
import logging

from argparse import ArgumentParser

from boto.s3.connection import S3Connection

# HCP #facepalm
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

logging.getLogger("boto").setLevel(logging.WARNING)


def prepare_client():
    """Prepare a client to connect bucket (namespace)
     with the information from config json file provided by command line argument

    If any exception rasied inside the function, the script will exit.

    :return tuple, first is Namespace instance, the second is conf dict in case there are extra information
    """
    if len(sys.argv) < 2:
        print('Missing positional argument of path to config json file.')
        sys.exit(0)

    try:
        conf_path = sys.argv[1]
        with open(conf_path, 'r') as fha:
            conf = json.load(fha)
    except Exception:
        sys.exit('Failed to read conf json file %s' % conf_path)

    try:
        namespace_client = Namespace(conf['ID'], conf['SECRET'], conf['ENDPOINT'], conf['BUCKET'])
        return namespace_client, conf
    except Exception:
        sys.exit('Cannot create bucket: make sure names of key are correct.')


class Namespace:
    """Manage a namespace on Hitachi Content Platform using boto"""
    def __init__(self, aws_id, aws_secret, server, bucket):
        hs3 = S3Connection(aws_access_key_id=aws_id,
                           aws_secret_access_key=aws_secret,
                           host=server)
        self.bucket = hs3.get_bucket(bucket)

    def exists(self, name):
        return name in self.bucket

    def put(self, name, data):
        """Store a string to a key

        :param str name: name of the key to be created
        :param str data: content to be written to the named key
        :return the number of bytes written to the key
        """
        # without versioning enabled to a namespace, same file name
        # will cause 409 Conflict error
        return self.bucket.new_key(name).set_contents_from_string(data)

    def upload(self, path, name=None):
        """Upload a file to a key
        :param str path: path of file, if the path is full path, all directories are created
        :param str name: name of key. Default None, which means path of file is the key
        :return the number of bytes written to the key
        """
        # without versioning enabled to a namespace, same file name
        # will cause 409 Conflict error
        if name:
            return self.bucket.new_key(name).set_contents_from_filename(path)
        return self.bucket.new_key(path).set_contents_from_filename(path)

    def get(self, name):
        """Get the content of a key as a string
        :param str name: name of key.
        :return str
        """
        return self.bucket.get_key(name,
                                   validate=False).get_contents_as_string()

    def download(self, name, path):
        """Get the content of a key as a string
        :param str name: name of key.
        :param str path: file name to store content to. Parent paths have to exist.
        :return str
        """
        self.bucket.get_key(name,
                            validate=False).get_contents_to_filename(path)

    def list(self, prefix='', marker='', folder_only=False):
        """List a namespace

        :param str prefix
        :param str marker: the key to start from
        :param bool folder_only: when True, list only one level, if there are two levels, you get directories
        :return keys
        """
        delimiter = '/' if folder_only else ''
        return self.bucket.list(prefix=prefix, marker=marker, delimiter=delimiter)

    def delete(self, name):
        """Delete a key one at a time

        Cannot delete a "folder" if it is not empty:
        I have not found -f argument for it
        """
        self.bucket.get_key(name).delete()


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s %(asctime)s %(filename)s.%(funcName)s +%(lineno)d: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(__name__)

    # with open('hcp_only_rw_config.json', 'r') as f:
    #     conf = json.load(f)
    # store_id = conf['ID']
    # store_secret = conf['SECRET']
    # store_url = conf['ENDPOINT']
    # bucket = conf['BUCKET']

    # client = Namespace(store_id, store_secret, store_url, bucket)
    client, conf = prepare_client()
    logger.debug(conf)

    # client.upload('/home/li/Documents/ersa/reporting-unified/bin/hcp.py', '/alder/nectar/hcp.py')
    # client.download('/alder/nectar/0/000000000-000156999.json.xz', '000000000-000156999.json.xz')
    # client.put('/dummy/filename', 'good')
    # client.delete('hcp.py')
    # client.delete('/dummy/filename')
    # client.delete('/dummy/')
    for item in client.list(folder_only=True):
        # logger.debug(dir(item))
        # logger.debug(item.name)
        logger.debug("%s", item.name)
        if hasattr(item, 'last_modified'):
            logger.debug("%s %s", item.name, item.last_modified)
        else:
            logger.debug("%s", item.name)
    # k = client.bucket.get_key('20160113-112448/tizard.pbs/1/000001867724-000001867808.json.xz')
    # logger.debug(dir(k))
    # logger.debug(k.last_modified)
    # # something is wrong here: GMT not GMT at all or mktime screwed ?
    # logger.debug(int(time.mktime(time.strptime(k.last_modified, '%a, %d %b %Y %H:%M:%S %Z'))))

    # for item in client.items('20160113-112448/tizard.pbs/1'):
    #     logger.debug(item)
