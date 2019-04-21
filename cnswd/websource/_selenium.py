"""
配置driver
    1. 下载Firefox浏览器
    2. 下载最新版本geckodriver，解压后移动到当前用户目录
         https://github.com/mozilla/geckodriver/releases
    3. 将geckodriver.exe所在目录添加path(win)

性能
    1. 初始化一个浏览器大约需要3~4秒

说明
    1. windows 10 edge不支持headless，使用firefox

"""
import os
import platform
import time

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from ..utils import data_root

LOG_PATH = os.path.join(data_root('geckordriver'), f'{os.getpid()}.log')


def make_headless_browser():
    """无头浏览器"""
    options = Options()
    options.headless = True
    # # 禁用gpu加速
    # options.add_argument('--disable-gpu')
    return webdriver.Firefox(options=options, service_log_path=LOG_PATH)


def make_headless_browser_with_auto_save_path(download_path, content_type):
    """带自定义下载路径的无头浏览器"""
    options = Options()
    options.headless = True
    fp = webdriver.FirefoxProfile()
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.manager.showWhenStarting", False)
    fp.set_preference("browser.download.dir", download_path)
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk", content_type)
    return webdriver.Firefox(options=options, firefox_profile=fp, service_log_path=LOG_PATH)
