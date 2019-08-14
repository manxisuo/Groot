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
from collections import Mapping
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
_todo_status = {}
_done_status = {}


def _put_task(task):
    task_flag = type(task).__name__
    _todo_status.setdefault(task_flag, 0)
    _todo_status[task_flag] += 1
    _queue.put(task)


def _get_task():
    task = _queue.get()
    _todo_status[type(task).__name__] -= 1
    return task


def _identify(sth, *args, **kwargs):
    return sth


# 打印INFO日志
def _info(msg):
    t = time.strftime('%Y-%m-%d %H:%M:%S')
    print('[INFO][{0}] {1}'.format(t, msg))


def _iterable(sth):
    return hasattr(sth, '__iter__')


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

    def extract(self, resp_str, page_inner_data):
        elements = self.selector.select(resp_str)
        for el in elements:
            context = {**el.attrs, '_text_': el.text}  # 特殊属性：'_text_'
            yield context


Tag = Element


# 正则表达式抽取器
class Re:
    def __init__(self, re_str):
        self.regexp = re.compile(re_str)

    def extract(self, resp_str, page_inner_data):
        for m in self.regexp.finditer(resp_str):
            val = m.group()
            context = {'#'+str(k+1): v for k, v in enumerate(m.groups())}
            context['#0'] = m.group()
            yield context


# 将多个抽取器顺序组合在一起的抽取器
# 通常应该：第一个是内置抽取器，其他的是自定义抽取器
class Chain:
    def __init__(self, *extractors):
        self.extractors = extractors

    def extract(self, resp_str, page_inner_data):

        def fn(_contexts, _extract):
            for _context in _contexts:
                yield from _extract(_context, page_inner_data)

        contexts = [resp_str]
        for extractor in self.extractors:
            contexts = fn(contexts, extractor.extract)

        yield from contexts


# 用户自定义抽取器或动作
class Func:
    def __init__(self, fn):
        self.fn = fn

    def act(self, context: dict, page_data: dict):
        self.fn(context)

    def extract(self, resp_str, page_inner_data):
        yield from self.fn(resp_str, page_inner_data)


# 什么都不做的抽取器或动作
class Nothing:
    def __init__(self):
        pass

    def act(self, context: dict, page_data: dict):
        pass

    def extract(self, resp_str, page_inner_data):
        yield {}


# 动作：URL入队列
# :custom 函数或格式化字符串
class Enqueue:
    def __init__(self, level, url_sof):
        self.level = level
        self.url_fn = _context_fn(url_sof)

    def act(self, context: dict, page_data: dict):
        url = self.url_fn(context)
        last_page_data = {**page_data['#outer'], **context['#outer']}
        _put_task(PageTask(self.level, url, last_page_data))


# 动作：下载
class Download:
    def __init__(self, url_sof, savedir_sof, filename_sof):
        self.url_fn = _context_fn(url_sof)
        self.savedir_fn = _context_fn(savedir_sof)
        self.filename_fn = _context_fn(filename_sof)

    def act(self, context: dict, page_data: dict):
        url = self.url_fn(context)

        action_ctx = {**context, **{
            '_basename_': os.path.basename(url),
            '_ext_': os.path.splitext(url)[1]
        }}

        task = DownloadTask(url, self.savedir_fn(action_ctx), self.filename_fn(action_ctx))
        _put_task(task)


class Scope(Enum):
    PAGE = 1
    ACTIONS = 2


# 动作：在某个作用范围内设置一个数据项
# :custom 函数或格式化字符串
class SetData:
    def __init__(self, scope, data_name, data_value_sof, keep=False):
        self.scope = scope
        self.data_name = data_name
        self.data_value_fn = _context_fn(data_value_sof)  # TODO 如果直接想从上下文中按名称取值怎么办
        self.keep = keep

    def act(self, context: dict, page_data: dict):
        data_value = self.data_value_fn(context)
        if self.scope == Scope.PAGE:
            if self.keep:
                page_data['#outer'][self.data_name] = data_value
            else:
                page_data['#inner'][self.data_name] = data_value
        elif self.scope == Scope.ACTIONS:
            context[self.data_name] = data_value
            if self.keep:
                context['#outer'][self.data_name] = data_value


# 将一个在当前上下文能取到的变量，保存在指定的作用范围内，并保持到下一级别的页面
class KeepData:
    def __init__(self, scope, data_name):
        self.scope = scope
        self.data_name = data_name

    def act(self, context: dict, page_data: dict):
        data_value = context[self.data_name]
        if self.scope == Scope.PAGE:
            page_data['#outer'][self.data_name] = data_value
        elif self.scope == Scope.ACTIONS:
            context['#outer'][self.data_name] = data_value


# 页面处理任务
class PageTask:
    def __init__(self, level, url, last_page_data=None):
        self.level = level
        self.url = url
        self.last_page_data = last_page_data or {}  # 上一个页面的页面数据
        self.need_sleep = True

    def tid(self):
        return self.url

    def run(self):
        _info('Get {0}'.format(urllib.parse.unquote(self.url)))

        page_data = {'#inner': {'_url_': self.url}, '#outer': {}}  # 当前页面的页面数据
        resp_str = str(_session.get(self.url).content, 'utf-8')
        rules = _rule_dict[self.level]

        for extractor, actions in rules:
            # 抽取结果
            contexts = extractor.extract(resp_str, page_data['#inner'])
            contexts = [*contexts]  # 为了计算结果数，所以转为list。TODO
            r_len = len(contexts)

            # 执行动作(列表)
            for index, ctx in enumerate(contexts):
                # 当前元素在抽取的元素列表中的索引，从1开始
                ctx['_index_'] = index + 1
                ctx['_len_'] = r_len
                ctx['#outer'] = {}

                # 将上一个页面的页面数据保存到context中
                ctx.update(self.last_page_data)

                # 将当前页面的页面数据保存到context中
                ctx.update(page_data['#inner'])
                ctx.update(page_data['#outer'])

                if not _iterable(actions):  # 单个action时，可以不放到列表中
                    actions = [actions]
                for action in actions:
                    action.act(ctx, page_data)


# 文件下载任务
class DownloadTask:
    def __init__(self, url, savedir, filename):
        self.url = url
        self.savedir = savedir
        self.filename = filename
        self.need_sleep = True

    def tid(self):
        return self.url

    def run(self):
        _info('Download {0}'.format(urllib.parse.unquote(self.url)))
        self.need_sleep = _download(self.url, self.savedir, self.filename)


def config(cfg: dict):
    _config.update(cfg)


def initial_urls(urls):
    for url in urls:
        _put_task(PageTask(1, url))


def page_rules(level, rules):
    if type(rules) is dict:
        rules = [(sel, rules[sel]) for sel in rules]
    _rule_dict[level] = rules


def page_rule(level, extractors, actions):
    _rule_dict.setdefault(level, [])
    _rule_dict[level].append((extractors, actions))


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


# 下载文件
# 返回值：是否真正下载
def _download(url, savedir, filename) -> bool:
    if not os.path.exists(savedir):
        try:
            os.makedirs(savedir)
        except FileExistsError:  # 由于多线程的原因，还是可能抛出异常
            pass

    file_path = os.path.join(savedir, filename)
    if not os.path.exists(file_path):
        # urllib.request.urlretrieve(url, file_path)
        resp = _session.get(url, stream=True)
        with open(file_path, 'wb') as fd:
            for chunk in resp.iter_content(chunk_size=128):  # TODO
                fd.write(chunk)
        return True
    else:
        return False


def _context_fn(str_or_fn):
    if type(str_or_fn) is str:
        return lambda ctx: str_or_fn.format_map(ctx)
    else:
        return str_or_fn


def _work_func():
    while True:
        task = _get_task()
        if task.tid() in _done_tasks:
            continue  # TODO 多线程同步问题
        _done_tasks.add(task.tid())
        task.run()

        task_flag = type(task).__name__
        _done_status.setdefault(task_flag, 0)
        _done_status[task_flag] += 1
        _queue.task_done()

        if task.need_sleep:
            time.sleep(_config['interval'])


def _monitor_func():
    while True:
        time.sleep(_config['status_log_interval'])
        _info('Status [DONE: {0} {1}, TODO: {2} {3}]'.format(sum(_done_status.values()), _done_status, sum(_todo_status.values()), _todo_status))


if __name__ == "__main__":
    pass