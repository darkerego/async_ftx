import asyncio
import json
import time
import urllib.parse

import aiohttp

from utils.customlogger import CustomLogger

class FtxRest:
    def __init__(self, API_KEY=None, API_SECRET=None, subAccount=None, host='https://ftx.com/api', loop=None,
                 logLevel='INFO', parse_float=float, *args, **kwargs):

        self.loop = loop or asyncio.get_event_loop()
        self.API_KEY = API_KEY
        self.API_SECRET = API_SECRET
        self.subAccount = subAccount
        self.host = host
        self.parse_float = parse_float
        self.logger = CustomLogger('FtxRest', logLevel=logLevel)

    def _generate_auth_headers(self, endpoint, params='', method='POST', body=None):
        """
        Generate headers for a signed payload
        """
        import hmac
        ts = int(time.time() * 1000)
        signature_payload = f'{ts}{method}/api/{endpoint}'
        if params != '':
            signature_payload += f'?{params}'
        if body is not None:
            signature_payload += body
        signature_payload = signature_payload.encode()
        signature = hmac.new(self.API_SECRET.encode(), signature_payload, 'sha256').hexdigest()
        prepared_headers = {
            "FTX-KEY": self.API_KEY,
            "FTX-SIGN": signature,
            "FTX-TS": str(ts),
        }

        if self.subAccount is not None:
            prepared_headers['FTX-SUBACCOUNT'] = self.subAccount

        return prepared_headers

    async def _process_response(self, response):
        try:
            text = await response.text()
            data = json.loads(text, parse_float=self.parse_float)
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(
                    '{} {} failed with status {} - {}'.format(response.method, response.url_obj.path, response.status,
                                                              data['error']))
            return data['result']

    async def fetch(self, endpoint, params=None):
        """
        Send a GET request to the ftx api

        @return response
        """
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        url = '{}/{}?{}'.format(self.host, endpoint, params)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await self._process_response(resp)

    async def fetch_auth(self, endpoint, params=None):
        """
        Send a pre-signed GET request to the ftx api

        @return response
        """
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        url = '{}/{}?{}'.format(self.host, endpoint, params)
        headers = self._generate_auth_headers(endpoint, params, 'GET')
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                return await self._process_response(resp)

    async def post(self, endpoint, data=None, params=None):
        """
        Send a pre-signed POST request to the ftx api

        @return response
        """
        if params is None:
            params = {}
        if data is None:
            data = {}
        params = urllib.parse.urlencode(params, safe='/')
        url = '{}/{}?{}'.format(self.host, endpoint, params)
        sData = json.dumps(data)
        headers = self._generate_auth_headers(endpoint, params, 'POST', sData)
        async with aiohttp.ClientSession() as session:
            async with session.post(url + params, headers=headers, json=data) as resp:
                return await self._process_response(resp)

    async def delete(self, endpoint, data=None, params=None):
        """
        Send a pre-signed DELETE request to the ftx api

        @return response
        """
        if params is None:
            params = {}
        if data is None:
            data = {}
        params = urllib.parse.urlencode(params, safe='/')
        url = '{}/{}?{}'.format(self.host, endpoint, params)
        sData = json.dumps(data)
        headers = self._generate_auth_headers(endpoint, params, 'DELETE', sData)
        async with aiohttp.ClientSession() as session:
            async with session.delete(url + params, headers=headers, json=data) as resp:
                return await self._process_response(resp)