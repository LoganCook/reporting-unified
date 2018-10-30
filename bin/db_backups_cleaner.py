#!/usr/bin/env python3

"""python script to manage database backups in AWS"""


import re
import time
import logging

from aws import prepare_client


def _reconstruct_backup_fn(db_name, backup_date):
    """Reconstruct key name of a database backup based on db name and date"""
    return '%s_%s.sql.xz' % (db_name, backup_date)


def _run():
    bucket, conf = prepare_client()

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    fmt = logging.Formatter(fmt='%(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    if 'LOG_LEVEL' in conf:
        logger.setLevel(conf['LOG_LEVEL'])
    else:
        logger.setLevel(logging.DEBUG)
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    backup_re = re.compile(r"(.+)_(20\d\d\d\d\d\d).sql.xz$")

    db_backups = {}

    logger.info('Start to clean database backups.')
    # Get the backups to be managed
    for item in bucket.list():
        fn = item.name
        result = backup_re.match(fn)
        if result:
            db_name, backup_date = result.groups()

            # make sure it is a date string
            try:
                time.strptime(backup_date, '%Y%m%d')
                logger.debug('%s backed up on %s', db_name, backup_date)
                if db_name in db_backups:
                    db_backups[db_name].append(backup_date)
                else:
                    db_backups[db_name] = [backup_date]
            except Exception:
                logger.warning('%s looks like a managed backup file, but failed strptime test.', fn)
        else:
            logger.debug('%s is not under management', fn)

    # Do a clean up if possible
    for db in db_backups:
        copies = len(db_backups[db])
        logger.debug('%s has %d backups.', db, copies)
        if copies > 1:
            backups = sorted(db_backups[db])
            to_keep = backups.pop()
            logger.debug('%s version is to keep.', to_keep)
            for item in backups:
                fn = _reconstruct_backup_fn(db, item)
                logger.info('%s is going to be deleted', fn)
                try:
                    bucket.delete(fn)
                    logger.info('%s has been deleted.', fn)
                except Exception:
                    logger.error('Failed to delete %s.', fn)

    logger.info('Finished cleaning database backups.')


_run()
