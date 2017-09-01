from celery import Celery
import os

amqp_user = os.getenv('AMQP_USER')
amqp_password = os.getenv('AMQP_PASSWORD')
amqp_hostname = os.getenv('AMQP_HOST')
amqp_port = os.getenv('AMQP_PORT')

app = Celery('joblauncher', broker='pyamqp://'+amqp_user+':'+amqp_password+'@'+amqp_hostname+':'+amqp_port+'//')


@app.task
def task_joblauncher(cmd):
    #print 'Task launched by celery worker'
    #cmd = 'joblauncher ' + ' '.join(args)
    #cmd = 'echo Tasklaunched'
    os.system('echo SUCCESS:TaskIsBeeingExecuted')
    #os.system(cmd)
