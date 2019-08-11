import requests
from bs4 import BeautifulSoup
from queue import Queue
import urllib.request
import urllib.parse
import time
import re
import sys
import os
import os.path
import threading
from collections import namedtuple


session = requests.Session()
session.headers['User-Agent'] = (
    'mozilla/5.0 (x11; linux x86_64) '
    'applewebkit/537.36 (khtml, like gecko) '
    'chrome/70.0.3538.102 safari/537.36')


q = Queue()  # URL队列
RULE_DICT = {}  # 用户注册的各级页面处理逻辑
OLD_TASKS = set() # 已处理任务的标识列表
CONFIG = {
    'thread_num': 1,
    'interval': 1,  # 同一线程两次HTTP请求的间隔秒数
}


def identify(sth, *args, **kwargs):
    return sth


# 打印INFO日志
def _info(msg):
    t = time.strftime('%Y-%m-%d %H:%M:%S')
    print('[INFO][{0}] {1}'.format(t, msg))


def _iterable(sth):
    return hasattr(sth, '__iter__')


# 从页面抽取的数据结果
Result = namedtuple('Result', ['val', 'context'])


class Context:
    def __init__(self, *args, **kwargs):
        self.args = [*args]
        self.kwargs = kwargs

    def format(self, format_str):
        return format_str.format(*self.args, **self.kwargs)

    def _c(self, _type):
        if _type is int:
            return self.args
        elif _type is str:
            return self.kwargs
        else:
            raise Exception('error key type')

    def __getitem__(self, item):
        return self._c(type(item))[item]

    def __setitem__(self, key, value):
        self._c(type(key))[key] = value


# 页面处理任务
class PageTask:
    def __init__(self, level, url, last_page_data={}):
        self.level = level
        self.url = url
        self.last_page_data = last_page_data  # 上一个页面的页面数据

    def tid(self):
        return self.url

    def run(self):
        _info('Get {0}'.format(urllib.parse.unquote(self.url)))
        html = str(session.get(self.url).content, 'utf-8')
        rules = RULE_DICT[self.level]

        for extractor, actions in rules.items():
            # 抽取结果
            results = extractor.extract(html)
            page_data = {}  # 当前页面的页面数据

            # 执行动作(列表)
            for i, result in enumerate(results):

                # 将上一个页面的页面数据保存到context中，给当前页面的action使用
                for name in self.last_page_data:
                    result.context[name] = self.last_page_data[name]

                if not _iterable(actions):  # 单个action时，可以不放到列表中
                    actions = [actions]
                for action in actions:
                    action.act(result, page_data, i + 1)


# 文件下载任务
class DownloadTask:
    def __init__(self, url, save_dir, filename):
        self.url = url
        self.save_dir = save_dir
        self.filename = filename

    def tid(self):
        return self.url

    def run(self):
        _info('Download {0}'.format(urllib.parse.unquote(self.url)))
        _download(self.url, self.save_dir, self.filename)


# CSS选择器
class Selector:
    def __init__(self, selector: str, max_count=sys.maxsize):
        self.selector = selector
        self.max_count = max_count

    def select(self, soup: BeautifulSoup):
        elements = soup.select(self.selector)[: self.max_count]
        return elements


# 元素属性提取器
class Element:
    def __init__(self, selector: str, name=None, fn=identify):
        self.selector = Selector(selector) if type(selector) is str else selector
        self.name = name
        self.fn = fn

    def extract(self, html):
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        elements = self.selector.select(soup)
        for el in elements:
            context = Context(**el.attrs)
            context['#text'] = el.text  # 特殊属性：'#text

            val = self.fn(context.kwargs[self.name]) if self.name else ''  # fn的参数
            yield Result(val, context)


# 元素的文本抽取器
def Text(selector: str):
    return Element(selector, '#text')


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)

    def extract(self, html):
        for m in self.regexp.finditer(html):
            val = m.group()
            yield Result(val, Context(val, *m.groups()))


# 用户自定义抽取器或动作
class Func:
    def __init__(self, fn):
        self.fn = fn

    def act(self, result: Result, page_data: dict, index):
        self.fn(result)

    def extract(self, html):
        return self.fn(html)


# 动作：下载
class Download:
    def __init__(self, save_dir, filename, custom=None):
        self.save_dir = save_dir
        self.filename = filename
        self.fn = _get_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict, index):
        val, context = result
        url = self.fn(result) if callable(self.fn) else result.val

        # 用于格式化保存路径和文件名的上下文
        context = Context(*context.args, **context.kwargs, **{
            'basename': os.path.basename(url),
            'ext': os.path.splitext(url)[1],
            'index': index
        })

        save_dir = context.format(self.save_dir)
        filename = context.format(self.filename)

        task = DownloadTask(url, save_dir, filename)
        q.put(task)


def _download(url, save_dir, filename):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    urllib.request.urlretrieve(url, os.path.join(save_dir, filename))


def _get_format_fn(format_str: str):
    def fn(result: Result):
        return result.context.format(format_str)
    return fn


# 动作：设置页面数据
# :custom 函数或格式化字符串
class SetPageData:
    def __init__(self, data_name, custom):
        self.data_name = data_name
        self.fn = _get_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict, index):
        page_data[self.data_name] = self.fn(result)


# 动作：URL入队列
# :custom 函数或格式化字符串
class Enqueue:
    def __init__(self, level, custom=None):
        self.level = level
        self.fn = _get_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict, index):
        url = self.fn(result) if callable(self.fn) else result.val
        q.put(PageTask(self.level, url, page_data))


def login(login_url, params):
    _info('Login: {0}'.format(login_url))
    r = session.post(login_url, data=params)


# 注册初始URL
# @Deprecated
def register_urls(func):
    for url in func():
        q.put(PageTask(1, url))
    return func


def initial_urls(urls):
    for url in urls:
        q.put(PageTask(1, url))


# 注册不同级别的页面处理逻辑
# @Deprecated
def register_page(level):
    def decorator(func):
        RULE_DICT[level] = func()
        return func
    return decorator


def page_rules(level, rules):
    RULE_DICT[level] = rules


def _thread_func():
    while True:
        item = q.get()
        if item.tid() in OLD_TASKS:
            continue  # TODO 多线程同步问题
        OLD_TASKS.add(item.tid())
        item.run()
        q.task_done()
        time.sleep(CONFIG['interval'])


# 启动爬虫
def start():
    for i in range(CONFIG['thread_num']):
        t = threading.Thread(target=_thread_func)
        t.daemon = True  # TODO
        t.start()
    q.join()


if __name__ == "__main__":
    c = Context(1, 2, 3, name="Tom")

