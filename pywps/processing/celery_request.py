from socket import getfqdn
import logging
import os
from datetime import datetime
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def format_body_request(
        dockerim_name='',
        dockerim_version=None,
        registry_url='',
        input_data={},
        param_as_envar=True,
        volume_mapping={},
        queue_name='celery'):

    return {
        'dockerim_name': dockerim_name,
        'dockerim_version': dockerim_version,
        'registry_url': registry_url,
        'input_data': input_data,
        'param_as_envar': param_as_envar,
        'volume_mapping': volume_mapping,
        'queue_name': queue_name,
    }


class Request(object):
    """
    Container class for all attributes relative to an annotation request.
    An instance of this class is meant to exist only during the processing
    of a request. (Hence it's name).
    Also offers general helper functions in the context of the Vesta workgroup
    annotators. (Can be used elsewhere also).
    """
    body = None
    url = None
    current_progress = None
    process_version = None


    def __init__(self, body, task_handler, required_args=None, download=True):
        """
        Constructor.
        :param body: Body of request message as defined by Vesta-workgroup.
        :param task_handler: Task instance of a Celery application.
        :param required_args: Required argments in 'misc', expressed as a dict
                              where the key is the name of the arg and the
                              value is a description of it's use.
        """
        self.body = body
        self.logger = logging.getLogger(__name__)
        self.logger.info("Handling task")
        self.logger.debug("Body has contents %s", body)
        self.host = getfqdn()

        # Docker params
        self.registry_url = self.body['registry_url']
        self.dockerim_name = self.body['dockerim_name']
        self.dockerim_version = self.body['dockerim_version']
        self.volume_mapping = self.body['volume_mapping']

        # Cloud params
        self.queue_name = self.body['queue_name']

        # Process params
        self.input_data = self.body['input_data']

        # This variable won't be needed later, it is used here to adapt the way ogc-processing apps process its
        # parameters (as command line argument instead of environment variable)
        self.param_as_envar = self.body['param_as_envar']

        self.task_handler = task_handler
        self.start_time = datetime.now().strftime(DATETIME_FORMAT)


    def set_progress(self, progress):
        """
        Helper function to set the progress state in the Celery Task backend.
        :param progress: Progress value between 0 and 100.
        :type progress: int
        """
        self.logger.debug("Setting progress to value %s", progress)
        if not isinstance(progress, int):
            raise TypeError("Progress must be expressed as an int")
        if progress < 0 or 100 < progress:
            raise ValueError("Progress must be between 0 and 100")

        self.current_progress = progress
        if self.task_handler:
            meta = {'current': progress,
                    'total': 100,
                    'worker_id_version': self.process_version,
                    'start_time': self.start_time,
                    'host': self.host,}
            self.task_handler.update_state(state='PROGRESS', meta=meta)
        else:
            self.logger.warning("Could not set progress at back-end")

