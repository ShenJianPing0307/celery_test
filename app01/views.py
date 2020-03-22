from django.shortcuts import render
from django.http.response import JsonResponse
from app01.tasks import  Upload

# Create your views here.

def doTask(request):
    print('start task!')
    Upload.delay() #异步任务，这里不会卡住，尽管有延时
    print('end task!')
    return JsonResponse({"type":'success'})

