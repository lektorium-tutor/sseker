"""
1. Получить токен авторизации
2. Создать поток для curl
3. Создать поток слушателя
4. Слушатель получает данные от потока curl
5. Слушатель отправляет данные на эндпоинт
"""
import json
import logging
import multiprocessing
import os
import sys
import time

import certifi
import pycurl
import requests
from pytz import timezone

logger = logging.getLogger('SSEKER')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

EDUCONT_BASE_URL = os.getenv('EDUCONT_BASE_URL', 'https://api.dev.educont.ru')
LMS_BASE_URL = os.getenv('LMS_BASE_URL', 'http://local.lektorium.tv:8000')
tz = timezone('Europe/Moscow')
SSE_PATH = bytes(f'{EDUCONT_BASE_URL}/api/v1/public/sse/connect', 'UTF-8')  # bytes for pycurl
TOKEN_PATH = f'{LMS_BASE_URL}/lekt/api/token?path={SSE_PATH}&method=GET'
SSE_ENDPOINT = f'{LMS_BASE_URL}/lekt/api/sse'


def get_token():
    logger.warning(TOKEN_PATH)
    r = requests.get(TOKEN_PATH, )

    if r.status_code == 200:
        return r.json()
    raise RuntimeError('Can\'t obtain a token')


def get_headers():
    return ['Content-Type: text/event-stream', f'Authorization: {get_token()}']


if __name__ == '__main__':
    cmd = f'curl  --location --request GET {SSE_PATH} {get_headers()}'


    def sender(conn):
        c = pycurl.Curl()
        c.setopt(c.URL, SSE_PATH)
        c.setopt(c.WRITEFUNCTION, conn.send)
        c.setopt(pycurl.HTTPHEADER, get_headers())
        c.setopt(c.CAINFO, certifi.where())
        c.setopt(c.VERBOSE, True)
        c.perform()
        c.close()
        conn.close()
        sys.exit('Stream down')


    def receiver(conn):
        while 1:
            data = conn.recv().decode('utf-8')

            if len(data.strip()) < 10:
                continue

            if data.startswith('data:'):
                data = data[len('data:'):]

            data = json.loads(data)
            r = requests.post(SSE_ENDPOINT, json={'profile_id': data.get('profileId'), 'status': data.get('status')})
            if r.status_code == 200:# TODO: проверить статус отправки, отправить повторно n раз, если неуспешно
                logger.info(f'Recieved event: {data}')
            else:
                logger.error(f'Unsuccessfull request: {r.status_code} - {r.text}')


    sse_listener, api_feeder = multiprocessing.Pipe()

    stream = multiprocessing.Process(target=sender, args=(sse_listener,))
    handler = multiprocessing.Process(target=receiver, args=(api_feeder,))

    stream.start()
    handler.start()

    handler.join()

    while True:
        stream.join()
        if stream.is_alive():
            logger.warning("Stream is alive")
            time.sleep(1)
