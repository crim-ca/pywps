##################################################################
# Copyright 2016 OSGeo Foundation,                               #
# represented by PyWPS Project Steering Committee,               #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import os
import pywps.configuration as config
from pywps.processing.basic import Processing
from pywps.exceptions import SchedulerNotAvailable
from pywps import dblog
import json

import logging
LOGGER = logging.getLogger("PYWPS")


class Scheduler(Processing):
    """
    :class:`Scheduler` is processing implementation to run jobs on schedulers
    like slurm, grid-engine and torque. It uses the drmaa python library
    as client to launch jobs on a scheduler system.

    See: http://drmaa-python.readthedocs.io/en/latest/index.html
    """

    def start(self):
        self.job.wps_response.update_status('Submitting job ...', 0)
        # run remote pywps process
        jobid = self.run_job()
        self.job.wps_response.update_status('Your job has been submitted with ID %s'.format(jobid), 0)

    def run_job(self):
        LOGGER.info("Submitting job ...")
        try:
            import drmaa
            session = drmaa.Session()
            # init session
            session.initialize()
            # dump job to file
            dump_filename = self.job.dump()
            if not dump_filename:
                raise Exception("Could not dump job status.")
            # prepare remote command
            jt = session.createJobTemplate()
            jt.remoteCommand = os.path.join(
                config.get_config_value('processing', 'path'),
                'joblauncher')
            if os.getenv("PYWPS_CFG"):
                import shutil
                cfg_file = os.path.join(self.job.workdir, "pywps.cfg")
                shutil.copy2(os.getenv('PYWPS_CFG'), cfg_file)
                LOGGER.debug("Copied pywps config: %s", cfg_file)
                jt.args = ['-c', cfg_file, dump_filename]
            else:
                jt.args = [dump_filename]
            jt.joinFiles = True
            jt.outputPath = ":{}".format(os.path.join(self.job.workdir, "job-output.txt"))
            # run job
            jobid = session.runJob(jt)
            LOGGER.info('Your job has been submitted with ID %s', jobid)
            # show status
            import time
            time.sleep(1)
            LOGGER.info('Job status: %s', session.jobStatus(jobid))
            # Cleaning up
            session.deleteJobTemplate(jt)
            # close session
            session.exit()
        except Exception as e:
            raise SchedulerNotAvailable("Could not submit job: %s" % str(e))
        return jobid

class CeleryTaskCaller(Processing):
    """
    :class:`Scheduler` is processing implementation to run jobs on schedulers
    like slurm, grid-engine and torque. It uses the drmaa python library
    as client to launch jobs on a scheduler system.

    See: http://drmaa-python.readthedocs.io/en/latest/index.html
    """

    def start(self):
        self.job.wps_response.update_status('Submitting job ...', 0)
        # run remote pywps process
        jobid = self.run_job()
        self.job.process._set_uuid(jobid) # set UUID of the task instead of the wps_request
        stored = dblog.get_stored().count()
        self.job.process._store_process(stored, self.job.wps_request, self.job.wps_response)

        self.job.wps_response.update_status('Your job has been submitted with ID %s'.format(jobid), 0)


    def run_job(self):
        LOGGER.info("Submitting job ...")
        try:
            process_request = self.job.process._handler(self.job.wps_request, self.job.wps_response)
            IaaS_deploy_execute = process_request['cloud_params']['IaaS_deploy_execute']
            broker_host = IaaS_deploy_execute['BROKER_HOST']
            broker_port = IaaS_deploy_execute['BROKER_PORT']
            queue_name = IaaS_deploy_execute['QUEUE_NAME']

            from celery_utils import config_module_dict, CELERY_APP
            broker_username = config_module_dict['BROKER_ADMIN_UNAME']
            broker_password = config_module_dict['BROKER_ADMIN_PASS']

            new_broker_url = "amqp://{0}:{1}@{2}:{3}".format(
                broker_username,
                broker_password,
                broker_host,
                broker_port
            )

            config_module_dict_new_broker = config_module_dict
            config_module_dict_new_broker['CELERY']['BROKER_URL'] = new_broker_url
            CELERY_APP.config_from_object(config_module_dict_new_broker['CELERY'])

            task_name = '{}.{}'.format(CELERY_APP.main, 'joblauncher')
            job_result = CELERY_APP.send_task(
                task_name,
                queue=queue_name,
                args=(process_request['request_body'],))

            LOGGER.info('Your job has been submitted with ID %s', job_result.id)
        except Exception as e:
            raise SchedulerNotAvailable("Could not submit job: %s" % str(e))
        return job_result.id
