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
from itertools import groupby


session = requests.Session()
session.headers['User-Agent'] = (
    'mozilla/5.0 (x11; linux x86_64) '
    'applewebkit/537.36 (khtml, like gecko) '
    'chrome/70.0.3538.102 safari/537.36')

CONFIG = {
    'thread_num': 2,
    'interval': 1,  # 同一线程两次HTTP请求的间隔秒数
    'status_log_interval': 5,
}

q = Queue()  # URL队列 TODO maxsize
RULE_DICT = {}  # 用户注册的各级页面处理逻辑
done_tasks = set()  # 已处理任务的标识列表
q_status = {}
done_status = {}


def config(cfg: dict):
    CONFIG.update(cfg)


def _put_task(task):
    task_flag = type(task).__name__
    if task_flag not in q_status:
        q_status[task_flag] = 0
    q_status[task_flag] += 1

    q.put(task)


def _get_task():
    task = q.get()
    task_flag = type(task).__name__
    q_status[task_flag] -= 1

    return task


def _identify(sth, *args, **kwargs):
    return sth


# 打印INFO日志
def _info(msg):
    t = time.strftime('%Y-%m-%d %H:%M:%S')
    print('[INFO][{0}] {1}'.format(t, msg))


def _iterable(sth):
    return hasattr(sth, '__iter__')


# 从页面抽取的数据结果
Result = namedtuple('Result', ['val', 'context'])


# 能对Context进行[*c]和{**c}操作 TODO
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

    def __iter__(self):
        for item in self.args:
            yield item
        for key in self.kwargs:
            yield key

    def append(self, item):
        return self.args.append(item)


# 页面处理任务
class PageTask:
    def __init__(self, level, url, last_page_data=None):
        self.level = level
        self.url = url
        self.last_page_data = last_page_data or {}  # 上一个页面的页面数据

    def tid(self):
        return self.url

    def run(self):
        _info('Get {0}'.format(urllib.parse.unquote(self.url)))

        page_data = {'#inner': {}, '#outer': {}}  # 当前页面的页面数据
        resp_str = str(session.get(self.url).content, 'utf-8')
        rules = RULE_DICT[self.level]

        for extractor, actions in rules:
            # 抽取结果
            results = extractor.extract(resp_str)
            results = [*results]  # 为了计算结果数，所以转为list。TODO
            r_len = len(results)

            # 执行动作(列表)
            for index, result in enumerate(results):
                # 当前元素在抽取的元素列表中的索引，从1开始
                result.context['#index'] = index + 1
                result.context['#len'] = r_len
                result.context['#inner'] = {}
                result.context['#outer'] = {}

                # 将上一个页面的页面数据保存到context中
                result.context.kwargs.update(self.last_page_data)

                # 将当前页面的页面数据保存到context中
                result.context.kwargs.update(page_data['#inner'])
                result.context.kwargs.update(page_data['#outer'])

                if not _iterable(actions):  # 单个action时，可以不放到列表中
                    actions = [actions]
                for action in actions:
                    action.act(result, page_data)


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
    def __init__(self, selector, name=None, fn=_identify):
        self.selector = Selector(selector) if type(selector) is str else selector
        self.name = name
        self.fn = fn

    def extract(self, resp_str):
        soup = BeautifulSoup(resp_str, 'lxml', from_encoding='utf-8')
        elements = self.selector.select(soup)
        for el in elements:
            context = Context(**el.attrs)
            context['#text'] = el.text  # 特殊属性：'#text

            val = self.fn(context[self.name]) if self.name else ''  # fn的参数
            yield Result(val, context)


# 元素的文本抽取器
def Text(selector):
    return Element(selector, '#text')


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)

    def extract(self, resp_str):
        for m in self.regexp.finditer(resp_str):
            val = m.group()
            yield Result(val, Context(val, *m.groups()))


# 用户自定义抽取器或动作
class Func:
    def __init__(self, fn):
        self.fn = fn

    def act(self, result: Result, page_data: dict):
        self.fn(result)

    def extract(self, resp_str):
        return self.fn(resp_str)


# 动作：URL入队列
# :custom 函数或格式化字符串
class Enqueue:
    def __init__(self, level, custom=None):
        self.level = level
        self.fn = _get_result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        url = self.fn(result) if callable(self.fn) else result.val
        last_page_data = {**result.context['#outer'], **page_data['#outer']}
        _put_task(PageTask(self.level, url, last_page_data))


# 动作：设置对同一Result的动作间数据
# :custom 函数或格式化字符串
class SetActionData:
    def __init__(self, data_name, custom):
        self.data_name = data_name
        self.fn = _get_result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        result.context['#inner'][self.data_name] = self.fn(result)


class KeepActionData:
    def __init__(self, data_name, custom=None):
        self.data_name = data_name

        if custom:
            self.fn = _get_result_format_fn(custom) if type(custom) is str else custom
        else:
            self.fn = lambda r: r.context[data_name]

    def act(self, result: Result, page_data: dict):
        result.context['#outer'][self.data_name] = self.fn(result)


# 动作：设置页面数据
# :custom 函数或格式化字符串
class SetPageData:
    def __init__(self, data_name, custom):
        self.data_name = data_name
        self.fn = _get_result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        page_data['#inner'][self.data_name] = self.fn(result)


class KeepPageData:
    def __init__(self, data_name, custom):
        self.data_name = data_name
        self.fn = _get_result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        page_data['#outer'][self.data_name] = self.fn(result)


# 动作：下载
class Download:
    def __init__(self, save_dir, filename, custom=None):
        self.save_dir = _get_context_format_fn(save_dir) if type(save_dir) is str else save_dir
        self.filename = _get_context_format_fn(filename) if type(filename) is str else filename
        self.fn = _get_result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        val, context = result
        url = self.fn(result) if callable(self.fn) else val

        # 用于格式化保存路径和文件名的上下文
        context = Context(*context.args, **context.kwargs, **{
            '#basename': os.path.basename(url),
            '#ext': os.path.splitext(url)[1]
        })

        task = DownloadTask(url, self.save_dir(context), self.filename(context))
        _put_task(task)


def _download(url, save_dir, filename):
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except FileExistsError:  # 由于多线程的原因，还是可能抛出异常
            pass

    file_path = os.path.join(save_dir, filename)
    if not os.path.exists(file_path):
        urllib.request.urlretrieve(url, file_path)


def _get_result_format_fn(format_str: str):
    def fn(result: Result):
        return result.context.format(format_str)
    return fn


def _get_context_format_fn(format_str: str):
    def fn(ctx: Context):
        return ctx.format(format_str)
    return fn


def login(login_url, params):
    _info('Login: {0}'.format(login_url))
    r = session.post(login_url, data=params)


# 注册初始URL
# @Deprecated
def register_urls(func):
    for url in func():
        _put_task(PageTask(1, url))
    return func


def initial_urls(urls):
    for url in urls:
        _put_task(PageTask(1, url))


# 注册不同级别的页面处理逻辑
# @Deprecated
def register_page(level):
    def decorator(func):
        RULE_DICT[level] = func()
        return func
    return decorator


def page_rules(level, rules):
    if type(rules) is dict:
        rules = [(sel, rules[sel]) for sel in rules]
    RULE_DICT[level] = rules


def _work_func():
    while True:
        task = _get_task()
        if task.tid() in done_tasks:
            continue  # TODO 多线程同步问题
        done_tasks.add(task.tid())
        task.run()

        task_flag = type(task).__name__
        if task_flag not in done_status:
            done_status[task_flag] = 0
        done_status[task_flag] += 1

        q.task_done()
        time.sleep(CONFIG['interval'])


def _monitor_func():
    while True:
        time.sleep(CONFIG['status_log_interval'])
        _info('Status [DONE: {0} {1}, TODO: {2} {3}]'.format(len(done_tasks), done_status, q.qsize(), q_status))


# 启动爬虫
def start():
    for i in range(CONFIG['thread_num']):
        t = threading.Thread(target=_work_func)
        t.daemon = True  # TODO
        t.start()
    threading.Thread(target=_monitor_func, daemon=True).start()  # 启动监控线程
    q.join()


if __name__ == "__main__":
    c = Context(1, 2, 3, name="Tom")

