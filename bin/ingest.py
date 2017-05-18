#!/usr/bin/env python3

# pylint: disable=C0111,W0212,W0611,E1102,W0703

"""
Ingestion Tool

Modified from ersa-reporting-ingest
"""

import concurrent.futures
import logging
import json
import lzma
import os
import time
import random

from argparse import ArgumentParser

import requests

from hcp import Namespace


logger = logging.getLogger('reporting-ingest')

DEBUG = True
TIMEOUT = 10  #timeout of request.get


class Ingester:
    """
      Ingest from object store or from local files

      From object store, it compares the list of objects in store and entries in
      input table of database. It can be slow.

      From local: upload a list of local files
    """
    def __init__(self, conf):
        try:
            self.endpoint = conf['DB_API']['ENDPOINT']
            self.token = conf['DB_API']['TOKEN']
            self.schema = conf['DB_API'].get('SCHEMA', '')

            store_id = conf['HCP']['ID']
            store_secret = conf['HCP']['SECRET']
            store_url = conf['HCP']['ENDPOINT']
            bucket = conf['HCP']['BUCKET']
        except Exception as e:
            raise KeyError("Configuration key error: %s" % str(e))

        self.prefix = conf['HCP'].get('PREFIX', '')
        self.substring = conf['HCP'].get('SUBSTRING', '')

        try:
            self.hcp = Namespace(store_id, store_secret, store_url, bucket)
        except Exception:
            raise ConnectionError("Cannot connect object store.")

        logger.debug('Ingest from store prefix %s into %s', self.prefix, self.endpoint)

    def _make_request(self, query):
        url = "%s/%s" % (self.endpoint, query)
        try:
            return requests.get(url,
                                headers={"x-ersa-auth-token": self.token},
                                timeout=TIMEOUT)
        except requests.ConnectTimeout:
            logger.warning('Request to %s timed out', query)
            return None

    @staticmethod
    def _verify_exist(rst):
        """Check the response for verifying existence"""
        if not rst:
            return False

        if rst.status_code == 204:
            # ingested successfully will receive 204 not 200
            return True
        elif rst.status_code == 200:
            return len(rst.json()) > 0

        logger.error("HTTP error %d", rst.status_code)
        return False

    def check_input(self, name):
        query = "input?filter=name.eq.%s" % name
        logger.debug(query)
        rst = self._make_request(query)
        return self._verify_exist(rst)

    def _put(self, name, data):
        return requests.put("%s/ingest?name=%s" % (self.endpoint, name),
                            headers={
                                "content-type": "application/json",
                                "x-ersa-auth-token": self.token},
                            data=json.dumps(data))

    def fetch(self, name):
        logger.debug("Retrieve and decompress %s from HCP", name)
        return json.loads(lzma.decompress(self.hcp.get(name)).decode("utf-8"))

    def get_latest_input(self):
        """Get the latest input from database

        This can be used as a marker for list objects from HCP
        """
        # TODO: this is not very helpful: we have to find out the latest of each partition
        query = "input?order=-name&count=1"
        logger.debug(query)
        rst = self._make_request(query)
        if self._verify_exist(rst):
            return rst.json()[0]['name']

        return ''

    def list_ingested(self):
        """List the ingested messages files in input table of database through API server"""
        page = 1
        names = []
        SIZE = 5000   # page size

        while True:
            url = "%s/input?count=%d&page=%s" % (self.endpoint, SIZE, page)
            try:
                batch = requests.get(url, headers={"x-ersa-auth-token": self.token}, timeout=TIMEOUT)
            except requests.ConnectTimeout:
                logger.warning('Query to DB for input timed out. url=%s', url)
                raise RuntimeError('Cannot connnect to DB')
            else:
                # This is for back compatibility in case 404 for no data
                if batch.status_code == 404:
                    break
                elif batch.status_code != 200:
                    raise IOError("HTTP %s" % batch.status_code)

                # new code always has status_code == 200 but json can be empty list
                records = batch.json()
                if len(records) > 0:
                    names += [item["name"] for item in batch.json()]
                    logger.debug("%d pages loaded", page)
                    page += 1
                    if len(records) < SIZE:
                        break
                else:
                    break

        return names

    def _prepare_batch_list(self):
        """Query input table and process json.xz in hcp which have not been ingested"""
        # The list can be very long if prefix is not used
        logger.debug("Getting list of archived packages of messages from database through API")
        ingested = self.list_ingested()
        ingested = set(ingested)

        logger.debug("Get list of archived packages of messages from object store")
        all_items = [item.name for item in self.hcp.list(prefix=self.prefix)
                     if not item.name.endswith("/")]

        if self.substring:
            all_items = [item for item in all_items if self.substring in item]

        all_items = set(all_items)

        todo = list(all_items - ingested)

        logger.info("%s objects, %s already ingested, %s todo",
                    len(all_items), len(ingested.intersection(all_items)), len(todo))

        return todo

    def batch(self):
        # end point is defined in config json file
        # As individual job is registered in input, set comparison should be avoid when numbers are too high
        logger.debug("Preparing list")
        todo = self._prepare_batch_list()
        for name in todo:
            logger.debug(name)
            data = self.fetch(name)
            if self.schema:
                data = [item for item in data if item["schema"] == self.schema]

            tracking_name = name

            success = self._verify_exist(self._put(tracking_name, data))
            if not success:
                logger.error("%s was not ingested", name)

    def _log_put(self, tracking_name, data):
        success = self._verify_exist(self._put(tracking_name, data))
        if not success:
            logger.error("%s was not ingested", tracking_name)

    def put_single_xz(self, xz_name, input_name):
        # This deal with local files for debug purpose, it will ingest each message separately in a xz file
        with lzma.LZMAFile(xz_name) as f:
            data = json.loads(f.read().decode("utf-8"))

        self._log_put(input_name, data)

    def put_local_xz(self, xz_name, input_name, check=False):
        # By default not check if exist as it can be very slow
        if check and self.check_input(input_name):
            logger.debug("Cannot ingest %s because it has been ingested before as the name %s is found in database.", xz_name, input_name)
        else:
            self.put_single_xz(xz_name, input_name)


def read_conf(path):
    # Check that the configuration file exists
    if not os.path.isfile(path):
        raise Exception("Config file %s cannot be found" % path)

    with open(path, 'r') as f:
        conf = json.load(f)
    return conf


def parse_command(description='Ingest records from HCP to Database through API server'):
    parser = ArgumentParser(description=description)
    parser.add_argument('conf', default='config.json',
                        help='Path to config.json. Default = config.json')
    args = parser.parse_args()
    return read_conf(args.conf)
