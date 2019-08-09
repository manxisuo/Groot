import requests
from bs4 import BeautifulSoup
from queue import Queue
import urllib.request
import urllib.parse
import re
import os
import os.path
import threading
from collections import namedtuple

session = requests.Session()
session.headers['User-Agent'] = \
    'mozilla/5.0 (x11; linux x86_64) ' \
    'applewebkit/537.36 (khtml, like gecko) ' \
    'chrome/70.0.3538.102 safari/537.36'

q = Queue()  # URL队列
RULE_DICT = {}  # 用户注册的各级页面处理逻辑
OLD_URLS = set()


def identify(sth, *args, **kwargs):
    return sth


def info(msg):
    print('[INFO] {0}'.format(msg))


def _iterable(sth):
    return hasattr(sth, '__iter__')


# 从页面抽取的数据结果
Result = namedtuple('Result', ['val', 'context'])


class Context:
    def __init__(self, *args, **kwargs):
        self.args = [*args]
        self.kwargs = kwargs

    def add_arg(self, arg):
        self.args.append(arg)

    def add_kwarg(self, key, value):
        self.kwargs[key] = value

    def format(self, format_str):
        return format_str.format(*self.args, **self.kwargs)


# 页面处理任务
class PageTask:
    def __init__(self, level, url):
        self.level = level
        self.url = url

    def tid(self):
        return self.url

    def run(self):
        info('Get {0}'.format(urllib.parse.unquote(self.url)))
        html = str(session.get(self.url).content, 'utf-8')
        rules = RULE_DICT[self.level]()

        for extractor, actions in rules.items():
            # 抽取结果
            results = extractor.extract(html)

            # 执行动作(列表)
            for i, result in enumerate(results):
                if not _iterable(actions):  # 单个action时，可以不放到列表中
                    actions = [actions]
                for action in actions:
                    action.act(result, i + 1)


# 文件下载任务
class DownloadTask:
    def __init__(self, url, save_dir, filename):
        self.url = url
        self.save_dir = save_dir
        self.filename = filename

    def tid(self):
        return self.url

    def run(self):
        info('Download {0}'.format(urllib.parse.unquote(self.url)))
        _download(self.url, self.save_dir, self.filename)


# 元素的文本抽取器
class Text:
    def __init__(self, selector: str, fn=identify):
        self.selector = selector
        self.fn = fn

    def extract(self, html):
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        elements = soup.select(self.selector)
        for el in elements:
            val = self.fn(el.text)
            yield Result(val, Context(**el.attrs))


# 元素的属性抽取器
class Attr:
    def __init__(self, selector: str, name, fn=identify):
        self.selector = selector
        self.name = name
        self.fn = fn

    def extract(self, html):
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        elements = soup.select(self.selector)
        for el in elements:
            val = self.fn(el.attrs[self.name])
            yield Result(val, Context(**el.attrs))


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

    def act(self, result: Result, n):
        self.fn(result)

    def extract(self, html) -> list:
        return self.fn(html)


# 动作：下载
class Download:
    def __init__(self, save_dir, filename, custom=None):
        self.save_dir = save_dir
        self.filename = filename
        self.fn = _get_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, n):
        val, context = result
        url = self.fn(result) if callable(self.fn) else result.val

        # 用于格式化保存路径和文件名的上下文
        context = Context(*context.args, **context.kwargs, **{
            'basename': os.path.basename(url),
            'ext': os.path.splitext(url)[1],
            'n': n
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


# 动作：URL入队列
# :custom 函数或字符串(支持{0}, {1}等格式)
class Enqueue:
    def __init__(self, level, custom=None):
        self.level = level
        self.fn = _get_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, n):
        url = self.fn(result) if callable(self.fn) else result.val
        q.put(PageTask(self.level, url))


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
        if item.tid() in OLD_URLS:
            continue  # TODO 多线程同步问题
        OLD_URLS.add(item.tid())
        item.run()
        q.task_done()


# 启动爬虫
def start(thread_num=3):
    for i in range(thread_num):
        t = threading.Thread(target=thread_func)
        t.daemon = True  # TODO
        t.start()
    q.join()


if __name__ == "__main__":
    pass
