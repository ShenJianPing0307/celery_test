from celery.task import Task
import time

class Upload(Task):

    name = 'upload-task' #给任务一个名称

    def run(self, *args, **kwargs):
        time.sleep(2)
        print('run upload task')

class scrawl(Task):
    name = 'spider'

    def run(self, *args, **kwargs):
        time.sleep(3)
        print('执行爬取任务')
