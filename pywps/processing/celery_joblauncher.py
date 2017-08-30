from celery import Celery
import os
app = Celery('joblauncher', broker='pyamqp://guest@localhost//')


@app.task
def task_joblauncher(cmd):
    #print 'Task launched by celery worker'
    #cmd = 'joblauncher ' + ' '.join(args)
    #cmd = 'echo Tasklaunched'
    os.system(cmd)
