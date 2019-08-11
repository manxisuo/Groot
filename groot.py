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
from enum import Enum

_session = requests.Session()
_session.headers['User-Agent'] = (
    'mozilla/5.0 (x11; linux x86_64) '
    'applewebkit/537.36 (khtml, like gecko) '
    'chrome/70.0.3538.102 '
    'safari/537.36')

_config = {
    'thread_num': 2,
    'interval': 1,  # 同一线程两次HTTP请求的间隔秒数
    'status_log_interval': 3,
}

_rule_dict = {}  # 用户注册的各级页面处理逻辑
_queue = Queue()  # URL队列 TODO maxsize
_done_tasks = set()  # 已处理任务的标识列表
_queue_status = {}
_done_status = {}


# 如果key不存在，则加入到字典，并赋予初值；否则，什么都不做
def _ensue_key(dict_, key, initial_value):
    if key not in dict_:
        dict_[key] = initial_value


def _put_task(task):
    task_flag = type(task).__name__
    _ensue_key(_queue_status, task_flag, 0)
    _queue_status[task_flag] += 1

    _queue.put(task)


def _get_task():
    task = _queue.get()
    _queue_status[type(task).__name__] -= 1

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


# 上下文
# 不支持对Context进行[*c]和{**c}操作 TODO
class Context:
    def __init__(self, *args, **kwargs):
        self.args = [*args]
        self.kwargs = kwargs
        self.type_map = {int: self.args, str: self.kwargs}

    def format(self, format_str: str):
        return format_str.format(*self.args, **self.kwargs)

    def __getitem__(self, item):
        return self.type_map[type(item)][item]

    def __setitem__(self, key, value):
        self.type_map[type(key)][key] = value

    # 支持in操作符
    def __iter__(self):
        yield from self.args
        yield from self.kwargs

    def append(self, item):
        return self.args.append(item)


# CSS选择器
class Selector:
    def __init__(self, val: str, max_count=sys.maxsize):
        self.val = val
        self.max_count = max_count

    def select(self, resp_str: str):
        soup = BeautifulSoup(resp_str, 'lxml', from_encoding='utf-8')
        elements = soup.select(self.val)[: self.max_count]
        return elements


# 元素属性提取器
class Element:
    def __init__(self, selector):
        self.selector = Selector(selector) if type(selector) is str else selector

    def extract(self, resp_str):
        elements = self.selector.select(resp_str)
        for el in elements:
            context = Context(**el.attrs)
            context['#text'] = el.text  # 特殊属性：'#text
            yield Result(None, context)  # TODO


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)

    def extract(self, resp_str):
        for m in self.regexp.finditer(resp_str):
            val = m.group()
            kwargs = {'#'+str(k+1): v for k, v in enumerate(m.groups())}
            kwargs['#0'] = m.group()
            ctx = Context(**kwargs)
            yield Result(None, ctx)  # TODO


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
    def __init__(self, level, custom):
        self.level = level
        self.fn = _result_format_fn(custom) if type(custom) is str else custom

    def act(self, result: Result, page_data: dict):
        url = self.fn(result)
        last_page_data = {**page_data['#outer'], **result.context['#outer']}
        _put_task(PageTask(self.level, url, last_page_data))


# 动作：下载
class Download:
    def __init__(self, url, save_dir, filename):
        self.save_dir = _context_format_fn(save_dir) if type(save_dir) is str else save_dir
        self.filename = _context_format_fn(filename) if type(filename) is str else filename
        self.url = _result_format_fn(url) if type(url) is str else url

    def act(self, result: Result, page_data: dict):
        context = result.context
        url = self.url(result)

        # 用于格式化保存路径和文件名的上下文
        ctx = Context(*context.args, **context.kwargs, **{
            '#basename': os.path.basename(url),
            '#ext': os.path.splitext(url)[1]
        })

        task = DownloadTask(url, self.save_dir(ctx), self.filename(ctx))
        _put_task(task)


class Scope(Enum):
    PAGE = 1
    ACTIONS = 2


# 动作：在某个作用范围内设置一个数据项
# :custom 函数或格式化字符串
class SetData:
    def __init__(self, scope, data_name, custom, keep=False):
        self.scope = scope
        self.data_name = data_name
        self.fn = _result_format_fn(custom) if type(custom) is str else custom
        self.keep = keep

    def act(self, result: Result, page_data: dict):
        if self.scope == Scope.PAGE:
            key_flag = '#outer' if self.keep else '#inner'
            page_data[key_flag][self.data_name] = self.fn(result)
        elif self.scope == Scope.ACTIONS:
            data_value = self.fn(result)
            result.context[self.data_name] = data_value
            if self.keep:
                result.context['#outer'][self.data_name] = data_value


# 将一个在当前上下文能取到的变量，保存在指定的作用范围内，并保持到下一级别的页面
class KeepData:
    def __init__(self, scope, data_name):
        self.scope = scope
        self.data_name = data_name

    def act(self, result: Result, page_data: dict):
        data_val = result.context[self.data_name]
        if self.scope == Scope.PAGE:
            page_data['#outer'][self.data_name] = data_val
        elif self.scope == Scope.ACTIONS:
            result.context['#outer'][self.data_name] = data_val


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
        resp_str = str(_session.get(self.url).content, 'utf-8')
        rules = _rule_dict[self.level]

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


def config(cfg: dict):
    _config.update(cfg)


def initial_urls(urls):
    for url in urls:
        _put_task(PageTask(1, url))


def page_rules(level, rules):
    if type(rules) is dict:
        rules = [(sel, rules[sel]) for sel in rules]
    _rule_dict[level] = rules


def login(login_url, params):
    _info('Login {0}'.format(login_url))
    r = _session.post(login_url, data=params)


# 启动爬虫
def start():
    for i in range(_config['thread_num']):
        t = threading.Thread(target=_work_func)
        t.daemon = True  # TODO
        t.start()
    threading.Thread(target=_monitor_func, daemon=True).start()  # 启动监控线程
    _queue.join()


def _download(url, save_dir, filename):
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except FileExistsError:  # 由于多线程的原因，还是可能抛出异常
            pass

    file_path = os.path.join(save_dir, filename)
    if not os.path.exists(file_path):
        urllib.request.urlretrieve(url, file_path)


def _result_format_fn(format_str: str):
    return lambda result: result.context.format(format_str)


def _context_format_fn(format_str: str):
    return lambda ctx: ctx.format(format_str)


def _work_func():
    while True:
        task = _get_task()
        if task.tid() in _done_tasks:
            continue  # TODO 多线程同步问题
        _done_tasks.add(task.tid())
        task.run()

        task_flag = type(task).__name__
        _ensue_key(_done_status, task_flag, 0)
        _done_status[task_flag] += 1

        _queue.task_done()
        time.sleep(_config['interval'])


def _monitor_func():
    while True:
        time.sleep(_config['status_log_interval'])
        _info('Status [DONE: {0} {1}, TODO: {2} {3}]'.format(len(_done_tasks), _done_status, _queue.qsize(), _queue_status))


if __name__ == "__main__":
    c = Context(1, 2, 3, name="Tom")

