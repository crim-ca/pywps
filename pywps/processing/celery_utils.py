import logging
import sys
import os
import imp
from celery import Celery

import pywps.configuration as config
from pywps import WPS, OWS
import time



ENCODING = 'utf-8'

dir_path = os.path.dirname(os.path.realpath(__file__))
default_configuration_path = dir_path+'/default_configuration.py'
config_file_path = os.getenv('VRP_CONFIGURATION', default_configuration_path)
config_module = imp.load_source('ogc_config', config_file_path)
config_module_dict = vars(config_module)

# This is the same function as in celery_init.py of ServiceGateway
def configure(config):
    proj_name = config['CELERY_PROJ_NAME']
    celery_app = Celery(proj_name)
    celery_app.config_from_object(config['CELERY'])
    return celery_app

CELERY_APP = configure(config_module_dict)

class WorkerExceptionWrapper(Exception):
    """
    Wrapper for worker exception
    """
    def __init__(self, task_uuid, task_status,
                 worker_exception,
                 worker_exc_traceback):

        self.task_uuid = task_uuid
        self.task_status = task_status
        self.worker_exception = worker_exception
        self.worker_exc_traceback = worker_exc_traceback
        w_e_msg = str(worker_exception).encode(ENCODING)
        super(WorkerExceptionWrapper, self).__init__(w_e_msg)

def get_request_info(uuid, app):
    """
    Get information on a processing request.

    :param uuid: UUID of a given request
    :param app: Handle to the Celery application.
    :returns: dict with information on request processing.
    """
    logger = logging.getLogger(__name__)
    # If uuid doesn't exist the status PENDING is returned
    # so it must be checked at a higher level if we don't want to tell user
    # that a task is pending even if it doesn't exist.

    logger.info("Obtaining information for task %s", uuid)
    async_result = app.AsyncResult(id=uuid)
    status = async_result.state
    result = async_result.result

    # Result for PENDING, PROGRESS and SUCCESS can be sent as is

    # For the moment I cannot validate the returned result for
    # RECEIVED and STARTED so force a None value as it's what
    # should be returned anyway
    logger.info("Task has status %s", status)
    if status == 'RECEIVED' or status == 'STARTED':
        result = None

    # FAILURE, RETRY and REVOKED status contain an exception in the result
    # object. The only difference is that RETRY state result has been
    # serialized so it must be reconstructed (use the exception_to_python
    # function as for the FAILURE state)
    # Raise the exception so that it can be handled at a higher level
    elif status == 'FAILURE' or status == 'RETRY' or status == 'REVOKED':
        if status == 'RETRY':
            result = async_result.backend.exception_to_python(result)

        exc_traceback = async_result.traceback
        raise WorkerExceptionWrapper(uuid, status, result, exc_traceback)

    information = {
        'uuid': uuid,
        'status': status,
        'result': result
    }
    return information

def cancel_request(uuid, app):
    """
    Cancel a processing request.

    :param uuid: UUID of a given request
    :param app: Handle to the Celery application.
    """
    logger = logging.getLogger(__name__)
    logger.info("Issuing a revoke command for task %s", uuid)
    app.control.revoke(uuid, terminate=True, signal='SIGKILL')


class VRPException(Exception):
    """
    Base exception type for current package.
    """
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

class AMQPError(VRPException):
    """
    Indicates that communications with AMQP failed.
    """
    def __init__(self):
        msg = "AMQP backend didn't response quickly enough."
        super(AMQPError, self).__init__(msg)


def async_fct_wrapper(out_dict, fct, *args, **kwargs):
    logger = logging.getLogger(__name__)
    try:
        logger.debug("fct : %s", fct)
        logger.debug("args : %s", args)
        logger.debug("kwargs : %s", kwargs)
        if "no_params_needed" in kwargs:
            logger.debug("Removing argument no_params_needed")
            kwargs.pop("no_params_needed")
        out_dict['return_value'] = fct(*args, **kwargs)
        logger.debug("out_dict : %s", out_dict)
    except:
        logger.exception("Threaded calling of Celery hit exception which "
                         "follows:",
                         exc_info=True)
        out_dict['exception'] = sys.exc_info()


import threading
def async_call(fct, *args, **kwargs):
    """
    Call AMQP functions with any arg or kwargs in an asynchronous manner.

    :param fct: The function to call asynchronously
    :param args: Arguments
    :param kwargs: Keyword arguments
    :return: The function output
    :raises: :py:exc:`~.vesta_exceptions.AMQPError`  if a timeout occurs
    """
    out_dict = {'return_value': None, 'exception': None}
    args_augmented = (out_dict, fct)
    args_augmented += args
    thr = threading.Thread(target=async_fct_wrapper,
                           args=args_augmented,
                           kwargs=kwargs)
    thr.start()
    thr.join(timeout=5)
    if thr.is_alive():
        raise AMQPError()

    if out_dict['exception'] is not None:
        exc = out_dict['exception']
        raise exc[0], exc[1], exc[2]

    return out_dict['return_value']


def uuid_task(task_id, status_or_cancel='status', service_route='.'):
    """
    Get the status or cancel a task identified by a UUID.

    :param status_or_cancel: status or cancel
    :param service_route: service route to obtain the requested service name
    :returns: JSON object with latest status or error response.
    :raises: :py:exc:`~.vesta_exceptions.MissingParameterError`
    """
    '''
    logger = logging.getLogger(__name__)
    service_name = validate_service_route(service_route)
    if not task_id:
        raise MissingParameterError('GET', '/{0}'.format(status_or_cancel), 'uuid')

    request_uuid = task_id
    log_request(service_name, '{op} on {uuid}'.format(op=status_or_cancel,
                                                      uuid=request_uuid))

    logger.info('%s request on task %s for %s',
                status_or_cancel, request_uuid, service_name)
    validate_uuid(request_uuid, service_name)
    '''
    request_uuid = task_id

    if status_or_cancel == 'cancel':
        async_call(cancel_request, request_uuid, CELERY_APP)

    state = async_call(get_request_info, request_uuid, CELERY_APP)

    #state = validate_state(request_uuid, service_name, state)

    return state


def format_progression(statusId, percentCompleted):
    return WPS.Status(
        WPS.ProcessStarted(
            statusId,
            percentCompleted=str(percentCompleted)
        ),
        creationTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
    )

def format_success(statusId):
    return WPS.Status(
        WPS.ProcessSucceeded(statusId),
        creationTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
    )

def format_failure(message):
    return WPS.Status(
        WPS.ProcessFailed(
            WPS.ExceptionReport(
                OWS.Exception(
                    OWS.ExceptionText(message),
                    exceptionCode='NoApplicableCode',
                    locater='None'
                )
            )
        ),
        creationTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
    )

def construct_status_doc_from_state(state):
    doc = WPS.ExecuteResponse()
    doc.attrib['{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'] = \
        'http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_response.xsd'
    doc.attrib['service'] = 'WPS'
    doc.attrib['version'] = '1.0.0'
    doc.attrib['{http://www.w3.org/XML/1998/namespace}lang'] = 'en-US'
    doc.attrib['serviceInstance'] = '%s%s' % (
        config.get_config_value('server', 'url'),
        '?service=WPS&request=GetCapabilities'
    )


    curr_status = state['status']

    file_url = config.get_config_value('server', 'outputurl')
    task_id = state['uuid']
    doc.attrib['statusLocation'] = os.path.join(file_url, str(task_id)) + '.xml'

    process_doc = WPS.Process(
        OWS.Identifier("ogc"),
        OWS.Title("ogc")
    )
    process_doc.attrib['{http://www.opengis.net/wps/1.0.0}processVersion'] = '1.0.0'
    doc.append(process_doc)

    if curr_status == 'PROGRESS':
        progression_percentage = state['result']['current']
        status_doc = format_progression(curr_status, progression_percentage)
        doc.append(status_doc)
    elif curr_status == 'SUCCESS':
        status_doc = format_success(curr_status)
        doc.append(status_doc)
        output_elements = WPS.Output(
            OWS.Identifier('output'),
            OWS.Title('output')
        )
        doc.append(WPS.ProcessOutputs(*output_elements))
        result_doc = WPS.Data()
        result = state['result']
        result_doc.text = str(result)
        doc.append(result_doc)
    elif curr_status == 'FAILURE':
        status_doc = format_failure(curr_status)
        doc.append(status_doc)
    else:
        status_doc = format_progression(curr_status, 0)
        doc.append(status_doc)

    return doc
