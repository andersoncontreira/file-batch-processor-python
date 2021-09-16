import json
import os
from datetime import datetime
from queue import Queue

from services.logging import get_logger
from os import listdir
from os import path


class BatchProcessor:
    def __init__(self, logger=None):

        self.logger = logger if logger is not None else get_logger()

        self.files = []
        self.supported_extensions = ['json']
        self.exception = None
        # num max of threads
        self.threads_count = 8
        # thread execution counter
        self.threads_item_counter = 0
        # queue size
        self.threads_queue_size = 500
        # execution key
        self.execution_key = "backup_{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
        # execution total items
        self.total_items = 0
        # total of requests
        self.total_request = 0
        # process queue
        self.queue = Queue()
        #
        self.success_items = []
        #
        self.error_items = []

        self.success_files = []

        self.error_files = []

        self.results = {
            'files': self.files,
            'total_files': len(self.files),
            'total_items': self.total_items,
            'total_request': 0,
            'total_items_per_thread': self.threads_queue_size
        }

    def read_dir(self, target):
        only_files = [path.join(target, f) for f in listdir(target) if path.isfile(path.join(target, f))]
        self.files = self.files + only_files

    def add_file(self, target):
        self.files.append(target)

    def process(self, callback):

        if len(self.files) > 0:

            self.results['files'] = self.files
            self.results['total_files'] = len(self.files)

            for file_name in self.files:
                # todo aplica threads por arquivo
                try:
                    self.logger.info("Processing file {}".format(file_name))
                    file_name_without_ext, file_extension = os.path.splitext(file_name)
                    file_extension = file_extension.replace('.', '')

                    if file_extension in self.supported_extensions:

                        file_data = self.read_file(file_name)
                        data = None
                        if file_extension == 'json':
                            data = self.read_json(file_data)

                        if data and len(data) > 0:

                            for item in data:
                                try:
                                    # self.logger.info('yep')
                                    callback(item)
                                except Exception as err:
                                    self.logger.error(err)
                                    self.error_items.append(item)

                        else:
                            error_message = 'Unable to read the data'
                            raise Exception(error_message)
                    else:
                        error_message = 'Unsupported file extension {}'.format(file_extension)
                        raise Exception(error_message)
                except Exception as err:
                    self.logger.error(err)
                    self.error_files.append({
                        "file": file_name,
                        "error": str(err)
                    })
                break

        else:
            self.logger.info("There is no files to process")



    def read_file(self, file_name):
        file_data = None
        try:
            with open(file_name, 'r') as f:
                file_data = f.read()
                f.close()
        except Exception as err:
            self.logger.error(err)
            self.exception = err

        return file_data

    def read_json(self, file_data):
        data = None
        try:
            data = json.loads(file_data)
        except Exception as err:
            self.logger.error(err)
            self.exception = err
        return data

    def get_results(self):
        return self.results

