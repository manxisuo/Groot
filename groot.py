import requests
from bs4 import BeautifulSoup
from queue import Queue
import urllib.request
import urllib.parse
import re
import os
import os.path
import threading

session = requests.Session()
session.headers['User-Agent'] = \
    'mozilla/5.0 (x11; linux x86_64) ' \
    'applewebkit/537.36 (khtml, like gecko) ' \
    'chrome/70.0.3538.102 safari/537.36'

q = Queue() # URL队列
RULE_DICT = {} # 用户注册的各级页面处理逻辑
OLD_URLS = set()


def identify(sth, *args, **kwargs):
    return sth


def info(msg):
    print('[INFO] {0}'.format(msg))


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
            if callable(extractor):  # 自定义extractor函数
                results = extractor(html)
            else:
                results = extractor.extract(html)

            # 执行动作(列表)
            for i, result in enumerate(results):
                val, context = result
                if not hasattr(actions, '__iter__'):  # 单个action时，可以不放到列表中
                    actions = [actions]
                for action in actions:
                    action.act(val, context, i + 1)


# 文件下载任务
class DownloadTask:
    def __init__(self, url, context, n, savedir, filename): # TODO
        self.url = url
        self.context = context
        self.n = n
        self.savedir = savedir
        self.filename = filename

    def tid(self):
        return self.url

    def run(self):
        info('Download {0}'.format(urllib.parse.unquote(self.url)))

        # 用于格式化保存路径和文件名的上下文
        context = {**self.context}
        context['basename'] = os.path.basename(self.url)
        context['ext'] = os.path.splitext(self.url)[1]
        context['n'] = self.n

        savedir = self.savedir.format(**context)
        filename = self.filename.format(**context)
        _download(self.url, savedir, filename)


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
            context = el.attrs
            yield (val, context)


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
            context = el.attrs
            yield (val, context)


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)

    def extract(self, html):
        for m in self.regexp.finditer(html):
            val = m.group()
            context = (val, *m.groups())
            yield (val, context)


# 动作：用户自定义
class Func:
    def __init__(self, fn):
        self.fn = fn

    def act(self, val, context, n):
        self.fn(val, context)  # TODO 怎样更灵活地传参数


# 动作：下载
class Download:
    def __init__(self, savedir, filename, fn=identify):
        """
        :param savedir: 文件保存目录
        :param filename: 文件名
        :param fn: (val, context) -> url 将提取的结果进行处理，得到下载URL
        """
        self.savedir = savedir
        self.filename = filename
        self.fn = fn

    def act(self, val, context, n):
        url = self.fn(val, context)  # TODO 函数的定位，以及参数
        task = DownloadTask(url, context, n, self.savedir, self.filename)
        q.put(task)


def _download(url, savedir, filename):
    if not os.path.exists(savedir):
        os.makedirs(savedir)
    urllib.request.urlretrieve(url, os.path.join(savedir, filename))


def _format_context(format_str: str, context) -> str:
    if format_str and context:
        if type(context) is tuple:
            return format_str.format(*context)
        elif type(context) is dict:
            return format_str.format(**context)
    return format_str


# 动作：URL入队列
# :custom 函数或字符串(支持{0}, {1}等格式)
class Enqueue:
    def __init__(self, level, custom=None):
        self.level = level
        self.custom = custom

    def act(self, val, context, n):
        if callable(self.custom):
            url = self.custom(val, context)  # TODO 函数的定位，以及参数
        elif type(self.custom) is str:
            url = _format_context(self.custom, context)
        else:
            url = val
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
