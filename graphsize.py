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

class CountGraphNodesJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def mapper(self, website, node):
        yield '_', website

        for outgoing in node['outgoing']:
            yield '_', outgoing

    def reducer(self, _, websites):
        yield 'count', str(len(set(websites)))


if __name__ == '__main__':
    CountGraphNodesJob().run()
