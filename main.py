import re
import io

import chardet
import geoip2.database
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

ip_pattern = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")

def fetch_ip_data(ip):
    reader = geoip2.database.Reader('GeoLite2-City.mmdb')
    try:
        response = reader.city(ip)
        data = {'country': response.country.name, 'city': response.city.name}
    except:
        data = {}
    reader.close()
    return (ip, data)

def find_duplicates(ip_data):
    ip_duplicates = defaultdict(list)
    for ip, data in ip_data.items():
        key = f"{data.get('city')}"
        ip_duplicates[key].append(ip)
    return ip_duplicates

with open('limboauth-v2.mv.db', 'rb') as f:
    byte_content = io.BufferedReader(f).read()

encoding = chardet.detect(byte_content)['encoding']
text = byte_content.decode(encoding)

ips = set(re.findall(ip_pattern, text))

with ThreadPoolExecutor() as executor:
    results = list(tqdm(executor.map(fetch_ip_data, ips), total=len(ips), desc='Fetching data'))

ip_data = dict(results)

ip_duplicates = find_duplicates(ip_data)

with open('duplicates.txt', 'w', encoding="utf-8") as f:
    for key, duplicates in tqdm(ip_duplicates.items(), desc='Writing to file'):
        if len(duplicates) > 1:
            ip_data_first = ip_data[duplicates[0]]
            f.write('{} - {} - {}\n'.format(', '.join(duplicates), ip_data_first.get('country'), ip_data_first.get('city')))