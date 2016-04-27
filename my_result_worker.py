# coding=utf-8

"""自定义的结果处理文件
设置redis中的状态
网址: state
state可以取0,1,2几个状态
0：requested，
1：saved，
2：uploaded（deleted）
"""

from pyspider.result import ResultWorker

import json
from os.path import join
import redis
import PIL.Image
import io
import base64
import re
import logging
logger = logging.getLogger("result")


class MyResultWorker(ResultWorker):
    save_folder = "/datadrive/spiderdata"
    redis_client = redis.StrictRedis()

    def on_result(self, task, res):
        assert task['taskid']
        assert task['project']
        assert task['url']
        assert res
        logger.info("[*] on_result: %s"%task['url'])

        # your processing code goes here
        if isinstance(res, list):
            for result in res:
                self.dump(result)
        elif isinstance(res, dict):
            self.dump(res)
        else:
            raise Exception("Wrong result type")

    def dump(self, result: dict):
        assert result['Type']
        assert result['Url']
        assert result['Title']
        assert result['Media']

        state = self.redis_client.get(result["Url"])
        if isinstance(state, int) and state > 0:
            return

        if result['Type'].startswith('joke'):
            self.dump_joke(result)
        elif result['Type'].startswith('pic'):
            self.dump_pic(result)
        else:
            raise Exception("Wrong type %s" % result['Type'])

    def dump_joke(self, result):
        assert result['Content']
        assert result['RawHtml']
        with open(join(self.save_folder, result['Type'], result['Title']), 'w', encoding='utf-8') as fp:
            json.dump(result, fp)
        self.redis_client.set(result["Url"], 1)

    def dump_pic(self, result):
        assert result['Binary']
        i = PIL.Image.open(io.BytesIO(result['Binary']))
        result["Width"], result['Height'] = i.size
        result["ImageType"] = "image/%s" % re.match(r"^PIL\.(\w+)ImagePlugin$", i.__module__).group(1).lower(),
        result["Base64Content"] = base64.encodebytes(result["Binary"]).decode('ascii')
        del result['Binary']
        with open(join(self.save_folder, result['Type'], result['Title']), 'w', encoding='utf-8') as fp:
            json.dump(result, fp)
        self.redis_client.set(result["Url"], 1)
