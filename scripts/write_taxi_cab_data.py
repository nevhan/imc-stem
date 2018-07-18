import gzip
import os
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import requests
import urllib3
from lxml import html
from minio import Minio
from minio.error import NoSuchKey

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

chunksize = 200 * 1024 ** 2  # 200MB
mc = Minio('minio.opt-pilot.svc.hkcc.ks:9000', 'opt-minio-dev', 'opt-minio-dev', secure=False)
bucket_name = 'stem-pres'

def get_data_links():
    resp = requests.get('http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml')
    tree = html.fromstring(resp.content)
    links = tree.xpath('//table[@class="table_style"]//a//@href')
    names = [Path(urlparse(link).path).name for link in links]
    return list(zip(links, names))


def minio_exists(bucket, f):
    try:
        mc.stat_object(bucket, f)
        return True
    except NoSuchKey:
        return False    


def main():
    for link, name in get_data_links():
        resp = requests.get(link, stream=True, verify=False)
        filename = '%s.gz' % name
        if minio_exists(bucket_name, filename):
            print('skipping %s' % filename)
            continue
        
        raw = BytesIO()

        print('{0} {1:0.2f} MB'.format(name, int(resp.headers['Content-Length']) / 1024 ** 2))
        
        with gzip.GzipFile(fileobj=raw, mode='wb') as f:
            for content in resp.iter_content(chunk_size=chunksize):
                print('  writing %s' % len(content))
                f.write(content)

        length = raw.tell()
        raw.seek(0)
        mc.put_object(bucket_name, filename, raw, length)


if __name__ == '__main__':
    main()
