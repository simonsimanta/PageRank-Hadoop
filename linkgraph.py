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



class MRCalculateLinkGraph(MRJob):

    #HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    #INPUT_PROTOCOL = RawValueProtocol
    @staticmethod
    def map_linkgraph(self,test):
        url = "https://commoncrawl.s3.amazonaws.com/"+test
        try:
            resp = requests.get(url, stream=True)
        except:
            pass
        try:
            for record in ArchiveIterator(resp.raw, arc2warc=True):

                if record.content_type != 'application/json':
                    continue
                try: 
                    try:
                        page_info = json.loads(record.content_stream().read())
                        page_url = page_info['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
                        page_domain = urlparse(page_url).netloc
                    except:
                        pass
                    links = page_info['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
                    try:
                        domains = set(filter(None, [urlparse(url['url']).netloc for url in links]))
                    except:
                        pass
                    if len(domains) > 0:
                            yield page_domain, list(domains)
                except(KeyError, UnicodeDecodeError):
                    pass
        except:
            pass

    def reduce_linkgraph(self, domain, links):
        links = list(set(itertools.chain(*links)))
        yield domain, {'id': domain, 'state': 0, 'diff':0, 'outgoing': links}



    def map_normalize(self, domain, data):
        yield data['id'], data



        for outgoing in data['outgoing']:
            yield outgoing, None



    def reduce_normalize(self, domain, data):
        self.increment_counter('linkgraph', 'size', 1)
        try:
            node = list(filter(lambda x: x is not None, data))[0]
        except IndexError:
            node = {'id': domain, 'state': 0, 'diff':0, 'outgoing': []}

        yield domain, node



    def steps(self):
        return [MRStep(mapper=self.map_linkgraph, reducer=self.reduce_linkgraph),MRStep(mapper=self.map_normalize, reducer=self.reduce_normalize)]



if __name__ == '__main__':
    MRCalculateLinkGraph().run()
