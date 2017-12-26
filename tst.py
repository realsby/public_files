#!/usr/bin/env python
import tornado.web
import tornado.ioloop
import datetime
import decimal
import json
import redis
from tornado.options import define, options, parse_command_line
from bson.objectid import ObjectId
from bson.json_util import default


def bson_to_json(o):
    if isinstance(o, (ObjectId, decimal.Decimal)):
        return str(o)
    elif isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
        r = o.isoformat()
        if isinstance(o, datetime.time):
            return r[:12]
        elif isinstance(o, datetime.datetime):
            return r + 'Z'
        return r
    return default(o)


class BaseHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def prepare(self):
        """Called at the beginning of a request before get/post/etc."""
        ip = self.request.headers.get("X-Forwarded-For", self.request.remote_ip)
        ip = self.request.headers.get("X-Real-Ip", ip.split(',')[-1].strip())
        self.ip = ip
        redis = self.settings['redis']
        timestamp = int(datetime.datetime.now().replace(year=1970, month=1, day=2, microsecond=0).strftime("%s"))
        timestamp -= timestamp % int(options.time_window / 2)
        redis.hincrby(ip, timestamp)
        redis.expire(ip, options.time_window)
        for k in redis.hscan_iter(ip):
            if int(k[0]) < timestamp - int(options.time_window / 2):
                redis.hdel(ip, k[0])
        if sum([int(x) for x in redis.hvals(ip)]) > options.request_limit:
            return self.send_error(424, reason="Too many request.")


class FirstRoute(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps([u"{}".format(k) for k in self.settings['redis'].hscan_iter(self.ip)],
                              default=bson_to_json))
        self.finish()


if __name__ == "__main__":
    define("ip", default="192.192.0.92", help="IP Address to listen on")
    define("port", default=8888, type=int)
    define("time_window", default=60, type=int, help="Time window as seconds (minimum:2-maximum:60)")
    define("request_limit", default=60, type=int, help="Maximum request in allowed window")
    define("redis_host", default='127.0.0.1')
    define("redis_port", default=6379, type=int)
    parse_command_line()
    redis_client = redis.StrictRedis(host=options.redis_host, port=options.redis_port)
    settings = {}
    settings['redis'] = redis_client

    handlers = [
        (r"/1/", FirstRoute),
    ]

    application = tornado.web.Application(handlers, **settings)
    application.listen(options.port, options.ip)
    tornado.ioloop.IOLoop.instance().start()
