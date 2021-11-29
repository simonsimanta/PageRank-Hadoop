
from gzip import GzipFile
from mrjob.job import MRJob
#from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol
import requests
from warcio.archiveiterator import ArchiveIterator
#from urllib3.util import parse_url as urlparse
from urllib.parse import urlparse
import ujson as json
import itertools
import math

class PageRankJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol


    def configure_args(self):
        super(PageRankJob, self).configure_args()
        self.add_passthru_arg('--graph-size', dest='size_of_web', default=0 )
        self.add_passthru_arg('--dangling-node-pr', dest='dangling_node_pr', default=0.0)
        self.add_passthru_arg('--damping-factor', dest='damping_factor', default=0.85)
        self.add_passthru_arg('--trust-factor', dest='trust_factor', default=0.0)

    def mapper(self, website, node):
        yield website, ('node', node)

        if 'gov' in website.split('.'):
            self.options.damping_factor = .10

        for outgoing in node['outgoing']:
            msg = node['state'] / len(node['outgoing'])
            yield outgoing, ('msg', msg)

    def reducer(self, website, data):
        node = None
        msgs = []

        for msg_type, msg_val in data:
            if msg_type == 'node':
                node = msg_val
            elif msg_type == 'msg':
                msgs.append(msg_val)

        if node != None:
            st1 = node['state']
            node['state'] = (float(self.options.damping_factor) + float(self.options.trust_factor)) * float(sum(msgs)) \
                    + (float(self.options.damping_factor) - float(self.options.trust_factor))* float(self.options.dangling_node_pr) / float(self.options.size_of_web) \
                    + (1 - float(self.options.damping_factor) - float(self.options.trust_factor)) / float(self.options.size_of_web)
            node['diff'] = math.fabs(st1 - node['state'] )

            yield website, node


if __name__ == '__main__':
    PageRankJob().run()
