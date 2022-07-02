"""
This module exposes the core ftx clients which includes both a rest interface client
"""
from typing import List, Optional, Dict

from ciso8601 import parse_datetime

from rest.rest_client import FtxRest


class Client:
    """
    The ftx client exposes rest objects
    """

    def __init__(self, API_KEY=None, API_SECRET=None, logLevel='INFO', *args, **kwargs):
        self.api_endpoint = 'https://ftx.com/api'
        self.referral = 'https://ftx.com/profile#a=blackmirror'
        self.rest = FtxRest(API_KEY=API_KEY, API_SECRET=API_SECRET, host=self.api_endpoint, logLevel=logLevel, *args, **kwargs)

    async def list_futures(self) -> List[dict]:
        return await self.rest.fetch('futures')

    async def list_perpetual_futures(self):
        """
        https://docs.ftx.com/#list-all-futures

        :return: a list contains all available perpetual futures
        """
        response = []
        for perpetual in await self.list_futures():
            if perpetual['perpetual'] is True:
                response.append(perpetual)

        return response

    async def list_markets(self) -> List[dict]:
        return await self.rest.fetch('markets')

    async def get_single_market(self, market):
        """
        https://docs.ftx.com/#get-single-market

        :param market: the trading market to query
        :return: a list contains single market info
        """

        return await self.rest.fetch(f"markets/{market.upper()}")

    async def get_orderbook(self, market: str, depth: int = None) -> dict:
        return await self.rest.fetch(f'markets/{market}/orderbook', {'depth': depth})

    async def get_trades(self, market: str) -> dict:
        return await self.rest.fetch(f'markets/{market}/trades')

    async def get_public_k_line(self, market: str, resolution=14400, limit=20, start_time: float = None,
                          end_time: float = None):
        """
        https://docs.ftx.com/#get-historical-prices

        :param market: the trading market to query
        :param resolution: the time period of K line in seconds
        :param limit: the records limit to query
        :param start_time: the target period after an Epoch time in seconds
        :param end_time: the target period before an Epoch time in seconds
        :return: a list contains all OHLC prices in exchange
        """

        query = {
            'resolution': resolution,
            'limit': limit,
        }

        if start_time is not None:
            query.update({
                'start_time': start_time,
            })

        if end_time is not None:
            query.update({
                'end_time': end_time
            })

        return await self.rest.fetch(f"markets/{market}/candles", query)

    async def get_account_info(self) -> dict:
        return await self.rest.fetch(f'account')

    async def get_open_orders(self, market: str = None) -> List[dict]:
        return await self.rest.fetch(f'orders', {'market': market})

    async def get_order_status(self, order_id):
        # https://docs.ftx.com/#get-order-status
        return await self.rest.fetch(f'orders', {"id": order_id})

    async def get_order_history(self, market: str = None, side: str = None, order_type: str = None, start_time: float = None,
                          end_time: float = None) -> List[dict]:
        return await self.rest.fetch(f'orders/history',
                         {'market': market, 'side': side, 'orderType': order_type, 'start_time': start_time,
                          'end_time': end_time})

    async def get_conditional_order_history(self, market: str = None, side: str = None, type: str = None,
                                      order_type: str = None, start_time: float = None, end_time: float = None) -> List[
        dict]:
        return await self.rest.fetch(f'conditional_orders/history',
                         {'market': market, 'side': side, 'type': type, 'orderType': order_type,
                          'start_time': start_time, 'end_time': end_time})

    async def modify_order(
            self, existing_order_id: Optional[str] = None,
            existing_client_order_id: Optional[str] = None, price: Optional[float] = None,
            size: Optional[float] = None, client_order_id: Optional[str] = None,
    ) -> dict:
        assert (existing_order_id is None) ^ (existing_client_order_id is None), \
            'Must supply exactly one ID for the order to modify'
        assert (price is None) or (size is None), 'Must modify price or size of order'
        path = f'orders/{existing_order_id}/modify' if existing_order_id is not None else \
            f'orders/by_client_id/{existing_client_order_id}/modify'
        return await self.rest.post(path, {
            **({'size': size} if size is not None else {}),
            **({'price': price} if price is not None else {}),
            **({'clientId': client_order_id} if client_order_id is not None else {}),
        })

    async def get_conditional_orders(self, market: str = None) -> List[dict]:
        return await self.rest.fetch(f'conditional_orders', {'market': market})

    async def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit',
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        return await self.rest.post('orders', {'market': market,
                                     'side': side,
                                     'price': price,
                                     'size': size,
                                     'type': type,
                                     'reduceOnly': reduce_only,
                                     'ioc': ioc,
                                     'postOnly': post_only,
                                     'clientId': client_id,
                                     })

    async def place_conditional_order(
            self, market: str, side: str, size: float, type: str = 'stop',
            limit_price: float = None, reduce_only: bool = False, cancel: bool = True,
            trigger_price: float = None, trail_value: float = None
    ) -> dict:
        """
        To send a Stop Market order, set type='stop' and supply a trigger_price
        To send a Stop Limit order, also supply a limit_price
        To send a Take Profit Market order, set type='trailing_stop' and supply a trigger_price
        To send a Trailing Stop order, set type='trailing_stop' and supply a trail_value
        """
        # print(market, side, size, type, limit_price, reduce_only, cancel, trigger_price, trail_value)
        assert type in ('stop', 'takeProfit', 'trailingStop')
        assert type not in ('stop', 'takeProfit') or trigger_price is not None, \
            'Need trigger prices for stop losses and take profits'
        assert type not in ('takeProfit',) or (trigger_price is None and trail_value is not None), \
            'Trailing stops need a trail value and cannot take a trigger price'

        return await self.rest.post('conditional_orders',
                          {'market': market, 'side': side, 'triggerPrice': trigger_price,
                           'size': size, 'reduceOnly': reduce_only, 'type': type,
                           'cancelLimitOnTrigger': cancel, 'orderPrice': limit_price, 'trailValue': trail_value})

    async def cancel_order(self, order_id: str) -> dict:
        return await self.rest.delete(f'orders/{order_id}')

    async def leverage(self, lev):
        return self.rest.post('account/leverage', {'leverage': lev})

    async def cancel_orders(self, market_name: str = None, conditional_orders: bool = False,
                      limit_orders: bool = False) -> dict:
        return await self.rest.delete(f'orders', {'market': market_name,
                                        'conditionalOrdersOnly': conditional_orders,
                                        'limitOrdersOnly': limit_orders,
                                        })

    async def get_fills(self) -> List[dict]:
        return await self.rest.fetch_auth(f'fills')

    async def get_balances(self) -> List[dict]:
        return await self.rest.fetch_auth('wallet/balances')

    async def get_deposit_address(self, ticker: str) -> dict:
        return await self.rest.fetch_auth(f'wallet/deposit_address/{ticker}')

    async def get_positions(self, show_avg_price: bool = False) -> List[dict]:
        return await self.rest.fetch_auth('positions', {'showAvgPrice': show_avg_price})

    async def get_position(self, name: str, show_avg_price: bool = False) -> dict:
        return next(filter(lambda x: x['future'] == name, await self.get_positions(show_avg_price)), None)

    async def get_all_trades(self, market: str, start_time: float = None, end_time: float = None) -> List:
        ids = set()
        limit = 100
        results = []
        while True:
            response = await self.rest.fetch(f'markets/{market}/trades', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            print(f'Adding {len(response)} trades with end time {end_time}')
            if len(response) == 0:
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        return results

    async def get_all_funding_rates(self) -> List[dict]:
        return self.rest.fetch('funding_rates')

    async def get_funding_payments(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return await self.rest.fetch('funding_payments', {
            'start_time': start_time,
            'end_time': end_time
        })

    async def get_subaccounts(self):
        return await self.rest.fetch_auth('subaccounts')

    async def create_subaccount(self, nickname: str) -> dict:
        return await self.rest.post('subaccounts', {'nickname': nickname})

    async def get_subaccount_balances(self, nickname: str) -> List[dict]:
        return await self.rest.fetch_auth(f'subaccounts/{nickname}/balances')

    async def transfer(self, coin: str, size: float, source: str, destination: str):
        return await self.rest.post(f'/subaccounts/transfer', {'coin': coin,
                                                     'size': size,
                                                     'source': source,
                                                     'destination': destination})

    async def get_deposit_history(self) -> List[dict]:
        return await self.rest.fetch_auth('wallet/deposits')

    async def get_withdrawal_fee(self, coin: str, size: int, address: str, method: str = None, tag: str = None) -> Dict:
        return await self.rest.fetch_auth('wallet/withdrawal_fee', {
            'coin': coin,
            'size': size,
            'address': address,
            'method': method,
            'tag': tag
        })

    async def get_withdrawals(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return await self.rest.fetch_auth('wallet/withdrawals', {'start_time': start_time, 'end_time': end_time})

    async def get_saved_addresses(self, coin: str = None) -> dict:
        return await self.rest.fetch_auth('wallet/saved_addresses', {'coin': coin})

    async def submit_fiat_withdrawal(self, coin: str, size: int, saved_address_id: int, code: int = None) -> Dict:
        return await self.rest.post('wallet/fiat_withdrawals', {
            'coin': coin,
            'size': size,
            'savedAddressId': saved_address_id,
            'code': code
        })

    async def get_latency_stats(self, days: int = 1, subaccount_nickname: str = None) -> Dict:
        return await self.rest.fetch('stats/latency_stats', {'days': days, 'subaccount_nickname': subaccount_nickname})

    async def otc_quote(self, _from: str, to: str, qty: float):
        return self.rest.post('otc/quotes', params={'fromCoin': _from, 'toCoin': to, 'size': qty})

    async def otc_convert(self, quote_id: str):
        return await self.rest.post(f'/otc/quotes/{quote_id}/accept')

