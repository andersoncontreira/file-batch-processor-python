from datetime import datetime
import sys, os
from os import path

from services import helper
from services.batch_processor import BatchProcessor
from services.logging import get_logger
import pytz

logger = get_logger()


def extract_event(item):
    return item.get('_source')


def execute_api_request(item):
    # logger.info('aq: {}'.format(item))
    event = extract_event(item)
    event['date'] = helper.datetime_format_for_lifecycle(datetime.now(pytz.timezone('America/Sao_Paulo')))
    del event['process']
    del event['squad']
    del event['company_id']
    del event['event_name']
    del event['system_name']
    logger.info(event)


def handler():
    target = sys.argv[1]

    logger.info("---------------------------------------------------------")
    logger.info("Beginning at {}".format(datetime.now(tz=pytz.timezone("America/Sao_Paulo"))))
    logger.info("---------------------------------------------------------")

    service = BatchProcessor()
    try:
        if path.isdir(target):
            logger.info('Target {} is a directory'.format(target))
            service.read_dir(target)
        elif path.isfile(target):
            logger.info('Target {} is a file'.format(target))
            service.add_file(target)
        else:
            raise FileNotFoundError("Target not found {}".format(target))

        service.process(execute_api_request)

    except Exception as err:
        logger.error(err)
    finally:
        result = service.get_results()

    logger.info("---------------------------------------------------------")
    logger.info("Finishing at {}".format(datetime.now(tz=pytz.timezone("America/Sao_Paulo"))))
    logger.info("---------------------------------------------------------")

    if result['total_files'] > 0:
        logger.info('Total processed files: {}'.format(result['total_files']))
        # logger.info('Total processed files with success: {}'.format(result['total_files']))
        # logger.info('Total processed files with error: {}'.format(result['total_files']))
        logger.info('Total processed items: {}'.format(result['total_items']))
        logger.info('Total processed requests: {}'.format(result['total_request']))
        # logger.info('Total items per file: {}'.format(result['total_items_per_file']))
        # logger.info("---------------------------------------------------------")
        # logger.info('{} - {} - {}'.format('bucket', 'bucket_file_name', 'uploaded?'))
        # logger.info("---------------------------------------------------------")

        # for res in result['items']:
        #     logger.info('{} - {} - {}'.format(res['bucket'], res['bucket_file_name'], res['uploaded']))

    logger.info("---------------------------------------------------------")
    logger.info("Exiting ...")
    logger.info("---------------------------------------------------------")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    handler()
