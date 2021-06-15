import pandas as pd
import urllib.request
import urllib.parse
import json
from multiprocessing import Process, Manager, Pool
import time

fname = r'm_vpn_addresses.xlsx'
data = pd.read_excel(fname)
df = pd.DataFrame(data, columns=['address'])
flen = len(df.index)
# flen=15
count = 1
mjson = {}
procs = []


def get_addr(id, mj):
    a = data.iat[id - 1, 6]
    url = 'https://geocode-maps.yandex.ru/1.x/?apikey=c0d403ab-e5be-4049-908c-8122a58acf23&format=json&geocode=' + \
          urllib.parse.quote_plus(a)
    response = urllib.request.urlopen(url)
    rec = response.read().decode('UTF8')
    rec = json.loads(rec)
    req1 = rec["response"]["GeoObjectCollection"]["metaDataProperty"]["GeocoderResponseMetaData"]["request"]
    req2 = rec["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["metaDataProperty"][
        "GeocoderMetaData"]["Address"]["formatted"]
    req3 = rec["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
    js = {"req": req1, "addr": req2, "pos": req3}
    mj[id] = js


if __name__ == '__main__':
    with Manager() as manager:
        pool = Pool(processes=8)
        jobs = []
        mjson = manager.dict()
        while count < flen + 1:
            p = pool.apply_async(func=get_addr, args=(count, mjson))
            jobs.append(p)
            while not all([p.ready() for p in jobs]):
                #    print (p.ready())
                time.sleep(0.1)
            if count % 100 == 1:
                json_object = mjson.copy()
                with open('dataN.txt', 'w', encoding='utf-8') as outfile:
                    json.dump(json_object, outfile, ensure_ascii=False)
            count += 1
            print(count)
        pool.close()
        pool.join()
        json_object = mjson.copy()
        with open('dataN.txt', 'w', encoding='utf-8') as outfile:
            json.dump(json_object, outfile, ensure_ascii=False)
