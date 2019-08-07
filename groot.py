import requests
from bs4 import BeautifulSoup
from queue import Queue
import urllib.request
import re
import os
import os.path
import threading


q = Queue() # URL队列
RULE_DICT = {} # 用户注册的各级页面处理逻辑

# 页面处理任务
class PageTask:
    def __init__(self, level, url):
        self.level = level
        self.url = url

    def run(self):
        html = requests.get(self.url).text
        rules = RULE_DICT[self.level]()

        for extractor, action in rules.items():

            if isinstance(extractor, Re): # 正则抽取模式
                vals = extractor.extract(None, html)
                for i, val in enumerate(vals):
                    if callable(action): # 自定义函数
                        action(val)
                    else:
                        action.act(val, None, i+1)

            else: # 元素抽取模式
                soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
                elements = soup.select(extractor.selector)
                for i, element in enumerate(elements): # TODO
                    val = extractor.extract(element, html)

                    if callable(action): # 自定义函数
                        action(val)
                    else:
                        action.act(val, element, i+1)


# 文件下载任务
class DownloadTask:
    def __init__(self, url, element, n, savedir, filename): # TODO
        self.url = url
        self.element = element
        self.n = n
        self.savedir = savedir
        self.filename = filename

    def run(self):
        # 用于格式化保存路径和文件名的上下文
        context = {**self.element.attrs} if self.element else {}
        context['basename'] = os.path.basename(self.url)
        context['ext'] = os.path.splitext(self.url)[1]
        context['n'] = self.n

        savedir = self.savedir.format(**context)
        filename = self.filename.format(**context)
        _download(self.url, savedir, filename)


# 元素的文本内容抽取器
class Text:
    def __init__(self, selector):
        self.selector = selector
    def extract(self, element, html):
        return element.text


# 元素的属性内容抽取器
class Attr:
    def __init__(self, selector, name):
        self.selector = selector
        self.name = name
    def extract(self, element, html):
        return element.attrs[self.name]


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)
    def extract(self, element, html):
        return self.regexp.findall(html)


# 动作：下载
class Download:
    def __init__(self, savedir, filename):
        self.savedir = savedir
        self.filename = filename
        
    def act(self, val, element, n):
        task = DownloadTask(val, element, n, self.savedir, self.filename)
        q.put(task)


def _download(url, savedir, filename):
    if not os.path.exists(savedir):
        os.makedirs(savedir)
    urllib.request.urlretrieve(url, os.path.join(savedir, filename))


# 动作：URL入队列
class Enqueue:
    def __init__(self, level):
        self.level = level
    def act(self, val, element, n):
        q.put(PageTask(self.level, val))


# 注册初始URL
def urls(func):
    for url in func():
        q.put(PageTask(1, url))
    return func


# 注册不同级别的页面处理逻辑
def page(level):
    def decorator(func):
        RULE_DICT[level] = func
        return func
    return decorator


def thread_func():
    while True:
        item = q.get()
        print(item)
        item.run()
        q.task_done()


# 启动爬虫
def start(thead_num=3):
    for i in range(thead_num):
        t = threading.Thread(target=thread_func)
        # t.daemon = True # TODO
        t.start()


if __name__ == "__main__":
    pass
