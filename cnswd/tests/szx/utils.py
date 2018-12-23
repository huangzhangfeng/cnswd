import tempfile
import time
import os
import hashlib
import matplotlib.image as mpimg

import cnswd

def get_expected_path(num, level):
    """测试文件路径"""
    r = os.path.dirname(cnswd.__file__)
    p = os.path.join(os.path.join(r,'tests','images'), '{}.{}.png'.format(num, level))
    return p


def get_actual_path(num, level):
    """临时文件路径"""
    r = tempfile.mkdtemp()
    return os.path.join(r,'{}.{}.png'.format(num, level))


def is_png_equal(p1, p2):
    """给定png路径的图片是否相等"""
    data_1 = mpimg.imread(p1)
    data_2 = mpimg.imread(p2)
    return hashlib.md5(data_1).hexdigest() == hashlib.md5(data_2).hexdigest()
