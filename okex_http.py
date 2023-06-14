"""
    火币httpClient
"""
import requests
from datetime import datetime
from enum import Enum
import hmac
import hashlib
import base64
from urllib.parse import urlencode, quote
import json
from requests import Response
from config import config
#from threading import Lock

class OkAccount(Enum):
    SPOT = "1"
    FUTURE = "3"
    MARGIN = "5"
    ASSET = "6"
    SWAP = "9"
    OPTION = "12"
    UNITE = "18"


class RequestMethod(Enum):
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"
    PUT = "PUT"

class InstType(Enum):
    SPOT='SPOT'
    SWAP='SWAP'
    FUTURES='FUTURES'
    OPTION='OPTION'

class OKHttp(object):
    """
    火币http client 公开和签名的接口.
    """

    def __init__(self, host=None, api_key=None, api_secret=None, passphrase=None, timeout=5):
        self.host = host if host else "https://www.okex.com"   # https://api.huobi.br.com # https://api.huobi.co
        self.api_host = 'www.okex.com'  # api.huobi.br.com
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = "IVERSON2021"
        self.timeout = timeout
        #self.lock  = Lock()

    def _request(self, method: RequestMethod, path, params=None, body=None, verify=False):

        url = self.host + path
        if params and not verify:
            url = url + '?' + self._build_params(params)
        #print(url)
        if verify:
            sign_data = self._sign(method.value, path, params)
            url = url + '?' + self._build_params(sign_data)
        headers = {"Content-Type": "application/json"}

        # print(url)
        if body:
            data = json.dumps(body)
        #print(url)
            response: Response = requests.request(method.value, url, headers=headers, params=params, data=data, timeout=self.timeout)
        else:
            response: Response = requests.request(method.value, url, headers=headers, timeout=self.timeout)

        #print(response)
        json_data = response.json()
        #print(json_data)

        if response.status_code == 200 and json_data['code'] == '0':
            return json_data
        else:
            raise Exception(f"请求{url}的数据发生了错误：{json_data}")


    def _build_params(self, params: dict):
        """
        构造query string
        :param params:
        :return:
        """
        return '&'.join([f"{key}={params[key]}" for key in params.keys()])

    def _sign(self, method, path, query_params=None, request_body=None):
        """

        :param method: GET or POST
        :param path:
        :return:
        """
        timestamp = self.get_timestamp()
        if query_params:
            path = path + '?' + urlencode(query_params)

        if request_body:
            data = json.dumps(request_body)  #
            msg = timestamp + method + path + data
        else:
            msg = timestamp + method + path
        #print(msg)
        digest = hmac.new(self.api_secret.encode('utf-8'), msg.encode('utf-8'), digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest).decode('utf-8')

        headers = {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.api_passphrase,
            'Content-Type': 'application/json'
        }

        return headers

    def get_timestamp(self):
        now = datetime.utcnow()
        timestamp = now.isoformat("T", "milliseconds")
        return timestamp + "Z"

    def get_exchange_info(self, instType):
        """
        此接口返回所有火币全球站支持的交易对。
        :return:
        """
        path = "/api/v5/public/instruments"

        params = {"instType": instType}
        #url = self.host + path
        #json_data = requests.get(url, timeout=self.timeout).json()
        # if json_data['status'] == 'ok':
        #     return json_data['data']
        #
        # raise Exception(f"请求{url}的数据发生错误, 错误信息--> {json_data}")

        return self._request(RequestMethod.GET, path, params=params)

    def get_currencys(self):
        """
        此接口返回所有火币全球站支持的币种。
        :return:
        """
        path = "/v1/common/currencys"
        # url = self.host + path
        # json_data = requests.get(url, timeout=self.timeout).json()
        # if json_data['status'] == 'ok':
        #     return json_data['data']
        #
        # raise Exception(f"请求{url}的数据发生错误, 错误信息--> {json_data}")

        return self._request(RequestMethod.GET, path)

    def get_exchange_timestamp(self):

        path = "/api/v5/public/time"
        # url = self.host + path
        # json_data = requests.get(url, timeout=self.timeout).json()
        # if json_data['status'] == 'ok':
        #     return json_data['data']
        #
        # raise Exception(f"请求{url}的数据发生错误, 错误信息--> {json_data}")

        return self._request(RequestMethod.GET, path)

    def get_funding_rate(self, instId):
        path =  '/api/v5/public/funding-rate'
        params = {"instId": instId}
        # url = self.host + path
        # json_data = requests.get(url, timeout=self.timeout).json()
        # if json_data['status'] == 'ok':
        #     return json_data['data']
        #
        # raise Exception(f"请求{url}的数据发生错误, 错误信息--> {json_data}")

        return self._request(RequestMethod.GET, path, params=params)





    def get_balances(self):
        path = '/api/v5/account/balance'
        headers = self._sign('GET', path)
        url = self.host + path
        json_data = requests.get(url, headers=headers,timeout=self.timeout).json()
        return json_data

    def get_positions(self, instType = None):
        path = '/api/v5/account/positions'
        params = {}
        url = self.host + path
        if instType:
            params['instType'] = instType
            url = url + self._build_params(params)
        headers = self._sign('GET', path)


        json_data = requests.get(url, headers=headers,timeout=self.timeout).json()
        return json_data

    def account_transfer(self, ccy, amt, account_from:OkAccount, account_to:OkAccount):
        path = "/api/v5/asset/transfer"
        body = {
            "ccy": ccy,
            "amt": amt,
            "from": account_from.value,
            "to": account_to.value
        }
        headers = self._sign('POST', path, request_body=body)
        url = self.host + path
        json_data = requests.post(url, data=json.dumps(body), headers=headers, timeout=self.timeout).json()
        return json_data

    def change_posmode(self,posmode):
        path = '/api/v5/account/set-position-mode'
        body = {
        "posmode": posmode
        }
        headers = self._sign('POST', path, request_body=body)
        url = self.host + path
        json_data = requests.post(url, data=json.dumps(body), headers=headers, timeout=self.timeout).json()
        return json_data

    def set_leverage(self, lever, mgnMode, instId=None, ccy=None):
        path = '/api/v5/account/set-leverage'
        body = {
            "lever": lever,
            "mgnMode": mgnMode
        }
        if instId:
            body['instId'] = instId
        if ccy:
            body['ccy'] = ccy
        headers = self._sign('POST', path, request_body=body)
        url = self.host + path
        json_data = requests.post(url, data=json.dumps(body), headers=headers, timeout=self.timeout).json()
        return json_data


    def place_order(self, instId, side, tdMode, ordType, sz, px=None, posSide=None):
        """
        type 为python关键字
        :param symbol: 如BTC-USDT
        :param side: buy or sell
        :param type: limit or market   # 7800
        :param price:
        :param amount:
        :return: type.
        """


        path = "/api/v5/trade/order"
        body = {
            "instId": instId,
            "side": side,
            "tdMode": tdMode,
            "ordType":ordType,
            #'posSide':'long'
        }
        if posSide:
            body['posSide'] = posSide

        if ordType == 'limit':
            if float(px) <= 0:
                raise ValueError('限价单要求价格不能为零')
            body['px'] = px
            if float(sz) <= 0:
                raise ValueError("限价单要求买卖数量要大于零")
            body['sz'] = sz
            #body['order_type'] = order_type

        if ordType == 'market':
            body['sz'] = sz
            #body['order_type'] = '0'
            #if side == 'buy':
                #if notional is None:
                #    raise ValueError('市价卖单要求需要指定下单的金额')
                #body['notional'] = notional
            #elif side == 'sell':
            #    if sz <= 0:
            #        raise ValueError("市价卖单需要指定卖出的数量")
            #    body['sz'] = sz

        headers = self._sign('POST', path, request_body=body)
        url = self.host + path
        json_data = requests.post(url, data=json.dumps(body), headers=headers, timeout=self.timeout).json()
        if json_data['code'] == '0':
            return 1
        else:
            print(json_data)
            return 0



if __name__ == '__main__':
    ok = OKHttp(api_key='16edc9cb-2d31-47ec-a3e7-6a7095ac41f2', api_secret='3A6E36C8A8B0B4C53FC9D57608C1385F')
    #for data in ok.get_exchange_info('SPOT')['data']:
    #    print(data)
    for pos in ok.get_positions()['data']:
        print(pos)
    #print(ok.change_posmode('net_mode'))
    #print(ok.place_order(instId='DOT-USDT-SWAP', side='sell', tdMode='cross', ordType='market', sz='1', posSide='short'))
    #print(ok.place_order(instId='DOT-USDT-SWAP', side='buy', tdMode='cross', ordType='market', sz='1', posSide='net'))
    #print(ok.account_transfer('USDT', 300, OkAccount.UNITE, OkAccount.ASSET))
    #print(ok.get_exchange_timestamp())
    #print(ok.place_order(instId='DOGE-USDT', side='buy', tdMode='cash', ordType='limit', sz='100', px='0.30'))

    #print(ok.set_leverage(lever="3", mgnMode="cross", instId='DOGE-USDT-SWAP'))
