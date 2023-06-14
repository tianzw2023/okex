
"""
    OKEX Websocket

"""

import json
import sys
import traceback
import socket
from datetime import datetime
from time import sleep
from threading import Lock, Thread
import websocket
import zlib
from config import config
import time



class OKEX_Websocket(object):
    """
        Websocket API
        创建Websocket client对象后，需要调用Start的方法去启动workder和ping线程
        1. Worker线程会自动重连.
        2. 使用stop方法去停止断开和销毁websocket client,
        3. 四个回调方法..
        * on_open
        * on_close
        * on_msg
        * on_error

        start()方法调用后，ping线程每隔60秒回自动调用一次。

    """

    def __init__(self, host=None, market_type = 'SPOT', ping_interval=20):
        """Constructor"""
        self.host = host
        self.ping_interval = ping_interval

        self._ws_lock = Lock()
        self._ws = None

        self._worker_thread = None
        self._ping_thread = None
        self._active = False  # 开启启动websocket的开关。

        # debug需要..
        self._last_sent_text = None
        self._last_received_text = None

        self.market_type = market_type
        self._callback = None

    def start(self):
        """
        启动客户端，客户端连接成功后，会调用 on_open这个方法
        on_open 方法调用后，才可以向服务器发送消息的方法.
        """

        self._active = True
        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

        self._ping_thread = Thread(target=self._run_ping)
        self._ping_thread.start()

    def set_callback(self, callback):
        self._callback = callback

    def stop(self):
        """
        停止客户端.
        """
        self._active = False
        self._disconnect()

    def join(self):
        """
        Wait till all threads finish.
        This function cannot be called from worker thread or callback function.
        """
        self._ping_thread.join()
        self._worker_thread.join()

    def send_msg(self, msg: dict):
        """
        向服务器发送数据.
        如果你想发送非json数据，可以重写该方法.
        """
        text = json.dumps(msg)
        self._record_last_sent_text(text)
        return self._send_text(text)

    def _send_text(self, text: str):
        """
        发送文本数据到服务器.
        """
        ws = self._ws
        if ws:
            ws.send(text, opcode=websocket.ABNF.OPCODE_TEXT)

    def _ensure_connection(self):
        """"""
        triggered = False
        with self._ws_lock:
            if self._ws is None:
                self._ws = websocket.create_connection(self.host)

                triggered = True
        if triggered:
            self.on_open()

    def _disconnect(self):
        """
        """
        triggered = False
        with self._ws_lock:
            if self._ws:
                ws: websocket.WebSocket = self._ws
                self._ws = None

                triggered = True
        if triggered:
            ws.close()
            self.on_close()

    def _run(self):
        """
        保持运行，知道stop方法调用.
        """
        try:
            while self._active:
                try:
                    self._ensure_connection()
                    ws = self._ws
                    if ws:
                        text = ws.recv()

                        # ws object is closed when recv function is blocking
                        if not text:
                            self._disconnect()
                            continue

                        self._record_last_received_text(text)

                        self.on_msg(text)
                # ws is closed before recv function is called
                # For socket.error, see Issue #1608
                except (websocket.WebSocketConnectionClosedException, socket.error):
                    self._disconnect()

                # other internal exception raised in on_msg
                except:  # noqa
                    et, ev, tb = sys.exc_info()
                    self.on_error(et, ev, tb)
                    self._disconnect()  #

        except:  # noqa
            et, ev, tb = sys.exc_info()
            self.on_error(et, ev, tb)

        self._disconnect()

    def _run_ping(self):
        """"""
        while self._active:
            try:
                self._ping()
            except:  # noqa
                et, ev, tb = sys.exc_info()
                self.on_error(et, ev, tb)
                sleep(1)

            for i in range(self.ping_interval):
                if not self._active:
                    break
                sleep(1)

    def _ping(self):
        """"""
        ws = self._ws
        if ws:
            ws.send("ping", websocket.ABNF.OPCODE_PING)

    def on_open(self):
        """on open """
        print("on open")
        if self.market_type == 'SPOTCOINSWAP':
            for symbol in config.symbols:
                instid_spot = symbol.upper()+'-USDT'
                instid_coinswap = symbol.upper()+'-USD-SWAP'
                data_spot = {"op": "subscribe","args": [{"channel": "books5","instId": instid_spot}]}
                data_coinswap = {"op": "subscribe","args": [{"channel": "books5","instId": instid_coinswap}]}
                self.send_msg(data_spot)
                self.send_msg(data_coinswap)
        elif self.market_type == 'SPOTUSDTSWAP':
            for symbol in config.symbols:
                instid = symbol.upper()+'-USDT-SWAP'
                instid_spot = symbol.upper() + '-USDT'
                data_spot = {"op": "subscribe","args": [{"channel": "books5","instId": instid_spot}]}
                data_usdtswap = {"op": "subscribe","args": [{"channel": "books5","instId": instid}]}
                self.send_msg(data_usdtswap)
                self.send_msg(data_spot)


    def on_close(self):
        """
        on close websocket
        """

    def on_msg(self, data: str):
        """call when the msg arrive."""
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
        )

        #msg = json.loads(decompress.decompress(data))
        #print(data)
        msg = json.loads(data)

        #print(msg)
        if 'data' in msg: #and msg['table'] == 'swap/depth5':
            data = msg['data']

            coin = {}
            coin['symbol'] = msg['arg']["instId"]
            if "USD-SWAP" in msg['arg']["instId"]:
                coin['market_type'] = 'COINSWAP'

            elif "USDT-SWAP" in msg['arg']["instId"]:
                coin['market_type'] = 'USDTSWAP'
            else:
                coin['market_type'] = 'SPOT'

            coin['time_send'] = float(data[0]['ts'])/1000
            coin['time_receive'] = time.time()
            coin['asks'] = [[float(ask[0]), float(ask[1])] for ask in data[0]['asks']]
            coin['bids'] = [[float(bid[0]), float(bid[1])] for bid in data[0]['bids']]
            coin['ask_quantity'] = coin['asks'][0][1]+coin['asks'][1][1]+coin['asks'][2][1]
            coin['ask_price'] = (coin['asks'][0][0]*coin['asks'][0][1]+coin['asks'][1][0]*coin['asks'][1][1]+coin['asks'][2][0]*coin['asks'][2][1])/coin['ask_quantity']
            coin['bid_quantity'] = coin['bids'][0][1] + coin['bids'][1][1] + coin['bids'][2][1]
            coin['bid_price'] = (coin['bids'][0][0] * coin['bids'][0][1] + coin['bids'][1][0] * coin['bids'][1][1] +coin['bids'][2][0] * coin['bids'][2][1]) / coin['bid_quantity']
            self._callback(coin)
            #print(coin)
    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """
        Callback when exception raised.
        """
        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb)
        )

        return sys.excepthook(exception_type, exception_value, tb)

    def exception_detail(
            self, exception_type: type, exception_value: Exception, tb
    ):
        """
        Print detailed exception information.
        """
        text = "[{}]: Unhandled WebSocket Error:{}\n".format(
            datetime.now().isoformat(), exception_type
        )
        text += "LastSentText:\n{}\n".format(self._last_sent_text)
        text += "LastReceivedText:\n{}\n".format(self._last_received_text)
        text += "Exception trace: \n"
        text += "".join(
            traceback.format_exception(exception_type, exception_value, tb)
        )
        return text

    def _record_last_sent_text(self, text: str):
        """
        Record last sent text for debug purpose.
        """
        self._last_sent_text = text[:1000]

    def _record_last_received_text(self, text: str):
        """
        Record last received text for debug purpose.
        """
        self._last_received_text = text[:1000]


if __name__ == '__main__':
    def callback(data):
        print(data)
    okex_ws = OKEX_Websocket(host="wss://ws.okex.com:8443/ws/v5/public", market_type= 'SPOTUSDTSWAP', ping_interval=20)
    okex_ws.set_callback(callback)
    okex_ws.start()

