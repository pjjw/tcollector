#!/usr/bin/env python

"""
graphite translator and tsd proxy
"""

import os,sys
import re
import string
import threading
import SocketServer

LISTEN_HOST = "localhost"
LISTEN_PORT = 44242

graphite_transforms = [
        (re.compile(r"(?P<begin>.*)\.jvm\.gc\.(?P<gc>\w*)\.(?P<end>.*)"),
            r"\g<begin>.jvm.gc.\g<end> gc=\g<gc>"),
        ]


class GraphiteProxyRequestHandler(SocketServer.StreamRequestHandler):

    def setup(self):
        SocketServer.StreamRequestHandler.setup(self)
        cur_thread = threading.current_thread()
        sys.stderr.write("client %s:%s connected\n" % (self.client_address[0],self.client_address[1]))


    def metric_transform(self, m):
        """
        Transform metric name according to defined regexps and format patterns.
        """
        tags = {}
        for (rexp,fmt) in graphite_transforms:
            match = rexp.match(m)
            if match:
                m = match.expand(fmt)
        fields = m.split()
        metric,tagstrs = (fields[0], fields[1:])
        for tag in tagstrs:
            tagf = tag.split("=")
            tags[tagf[0]] = tagf[1]
        return metric,tags


    def inputline(self, s):
        """
        handle a line of input
        """
        sys.stderr.write("%s: got: %s\n" %
                (self.client_address[0], s.strip()))
        fields = s.strip().split()
        # repeat anything with a "put" at the start
        if fields[0] == "put":
            sys.stdout.write(string.join(fields, " "))
            sys.stdout.write("\n")
        # ok, graphite then
        elif len(fields) == 3:
            (metric,value,epoch) = fields
            mt,tags = self.metric_transform(metric)
            # fix the ordering
            sys.stderr.write("graphite metric\n")
            sys.stdout.write(string.join((mt, epoch, value), " "))
            if tags:
                tagstr = ' ' + ' '.join("%s=%s" % (k,v) for k, v in
                        tags.iteritems())
                sys.stdout.write(tagstr)
            sys.stdout.write("\n")
        sys.stdout.flush()


    def handle(self):
        while 1:
            self.line = self.rfile.readline()
            if not self.line:
                sys.stderr.write("client %s disconnected\n" %
                        (self.client_address[0]))
                sys.stdout.flush()
                break
            self.inputline(self.line)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    daemon_threads = True
    # nasty hack because SocketServer doesn't clean up after itself.
    allow_reuse_address = True
    pass


if __name__ == "__main__":
    server = ThreadedTCPServer((LISTEN_HOST, LISTEN_PORT), GraphiteProxyRequestHandler)
    server.serve_forever()
    server.shutdown()

