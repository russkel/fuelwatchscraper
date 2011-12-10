'''
Created on 10/12/2011

@author: Russ
'''
#import scraperwiki, 
import yaml
import urllib2, lxml.etree
from datetime import datetime
import time

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        excpetions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            try_one_last_time = True
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                    try_one_last_time = False
                    break
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if try_one_last_time:
                return f(*args, **kwargs)
            return
        return f_retry  # true decorator
    return deco_retry


@retry(urllib2.HTTPError)
def fetch_page(prod, reg, date):
    return lxml.etree.parse(urllib2.urlopen(url % (prod, reg, date)))

url = "http://www.fuelwatch.wa.gov.au/fuelwatch/fuelWatchRSS?Product=%i&Region=%i&Day=%s"
# tomorrow's data is available after 14:30 WST (GMT+8) apparently

#Day:
#    today (this is the default)
#    tomorrow (only available after 2:30PM)

products = {
    1: 'Unleaded Petrol',
    2: 'Premium Unleaded',
    4: 'Diesel',
    5: 'LPG',
    6: '98 RON',
    7: 'B20 diesel'
}

regions = {
  25: 'Metro : North of River',
  26: 'Metro : South of River',
  27: 'Metro : East/Hills',
  15: 'Albany',
  28: 'Augusta / Margaret River',
  30: 'Bridgetown / Greenbushes',
  1: 'Boulder',
  2: 'Broome',
  16: 'Bunbury',
  3: 'Busselton (Townsite)',
  29: 'Busselton (Shire)',
  19: 'Capel',
  4: 'Carnarvon',
  33: 'Cataby',
  5: 'Collie',
  34: 'Coolgardie',
  35: 'Cunderdin',
  31: 'Donnybrook / Balingup',
  36: 'Dalwallinu',
  6: 'Dampier',
  20: 'Dardanup',
  37: 'Denmark',
  38: 'Derby',
  39: 'Dongara',
  7: 'Esperance',
  40: 'Exmouth',
  41: 'Fitzroy Crossing',
  17: 'Geraldton',
  21: 'Greenough',
  22: 'Harvey',
  42: 'Jurien',
  8: 'Kalgoorlie',
  43: 'Kambalda',
  9: 'Karratha',
  44: 'Kellerberrin',
  45: 'Kojonup',
  10: 'Kununurra',
  18: 'Mandurah',
  32: 'Manjimup',
  46: 'Meekatharra',
  47: 'Moora',
  48: 'Mt Barker',
  23: 'Murray',
  11: 'Narrogin',
  49: 'Newman',
  50: 'Norseman',
  12: 'Northam',
  13: 'Port Hedland',
  51: 'Ravensthorpe',
  14: 'South Hedland',
  53: 'Tammin',
  24: 'Waroona',
  54: 'Williams',
  55: 'Wubin',
  56: 'York'
}

dates = ['today']

#dates = [
#'8/12/2011',
#'9/12/2011',
#'10/12/2011'
#]

for date in dates:
    yamlfile = file("data/%s.yaml" % date.replace('/', '-'), 'a')
    
    for reg in regions.keys():
        for prod in products.keys():
            print "Fetching %s in %s for %s..." % (products[prod], regions[reg], date)
            tree = fetch_page(prod, reg, date)

            datalist = []
            for item in tree.xpath('/rss/channel/item'):
                #print lxml.etree.tostring(item)
                data = {
                    'trading-name': item.xpath('trading-name/text()')[0],
                    'brand': item.xpath('brand/text()')[0],
                    'address': item.xpath('address/text()')[0],
                    'suburb': item.xpath('location/text()')[0],
                    'phone': item.xpath('phone/text()'),
                    'latitude': item.xpath('latitude/text()'),
                    'longitude': item.xpath('longitude/text()'),
                    'date': datetime.strptime(item.xpath('date/text()')[0], "%Y-%m-%d"),
                    'price': item.xpath('price/text()')[0],
                    'product': products[prod],
                    'region': regions[reg]
                }

                for f in ['latitude', 'longitude', 'phone']:
                    if isinstance(data[f], list):
                        if len(data[f]) > 0:
                            data[f] = data[f][0]
                        else:
                            data[f] = None

                #print data
                datalist.append(data)

            #scraperwiki.sqlite.save(unique_keys=['trading-name', 'date'], data=datalist)
            yaml.dump(datalist, yamlfile)