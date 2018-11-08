#!/usr/bin/env python3

"""Download a file from AWS and save to curent directory
   for debugging.
"""

import sys
from argparse import ArgumentParser

from aws import Namespace
from ingest import read_conf


def parse_command(description='Download a file from AWS and save to curent directory'):
    parser = ArgumentParser(description=description)
    parser.add_argument('name', help='Key name of an object to be downloaded')
    parser.add_argument('--conf', default='debugger_api_conf.json', help='Path to config.json. Default = debugger_api_conf.json')
    args = parser.parse_args()
    return args.name, args.conf


if __name__ == "__main__":
    try:
        file_name, conf_file = parse_command()
    except Exception as e:
        print(e)
        sys.exit(2)

    conf = read_conf(conf_file)
    if 'AWS' in conf:
        conf = conf['AWS']

    namespace_client = Namespace(conf['ID'], conf['SECRET'], conf['ENDPOINT'], conf['BUCKET'])

    saved_file_name = file_name.split('/')[-1]
    namespace_client.download(file_name, saved_file_name)
    print('%s has been downloaded to current directory' % saved_file_name)
