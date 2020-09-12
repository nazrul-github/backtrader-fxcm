from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import threading
import copy
import time as _time
from datetime import datetime

import fxcmpy

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


class SerializableEvent(object):
    '''A threading.Event that can be serialized.'''

    def __init__(self):
        self.evt = threading.Event()

    def set(self):
        return self.evt.set()

    def clear(self):
        return self.evt.clear()

    def isSet(self):
        return self.evt.isSet()

    def wait(self, timeout=0):
        return self.evt.wait(timeout)

    def __getstate__(self):
        d = copy.copy(self.__dict__)
        if self.evt.isSet():
            d['evt'] = True
        else:
            d['evt'] = False
        return d

    def __setstate__(self, d):
        self.evt = threading.Event()
        if d['evt']:
            self.evt.set()


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class fxcmStore(with_metaclass(MetaSingleton, object)):
    '''Singleton class wrapping to control the connections to fxcm.
    Params:
      - ``token`` (default:``None``): API access token
      - ``demo`` (default: ``True``): use the test environment
      - ``account_poll_freq`` (default: ``5.0``): refresh frequency for account value/cash refresh
      - ``stream_timeout`` (default: ``2``): timeout for stream requests
      - ``poll_timeout`` (default: ``2``): timeout for poll requests
      - ``reconnections`` (default: ``-1``): try to reconnect forever connection errors
      - ``reconntimeout`` (default: ``5.0``): how long to wait to reconnect stream (feeds have own reconnection settings)
    '''
    params = dict(
        token='e2f4e3c5b861c438f5053d5347a3fa3c8b90242b',
        demo=True,
        # account balance refresh timeout
        account_poll_freq=5.0,
        # stream timeout
        stream_timeout=2,
        # poll timeout
        poll_timeout=2,
        # count of reconnections, -1 unlimited, 0 none
        reconnections=-1,
        # timeout between reconnections
        reconntimeout=5.0,
        # send store notification with recieved transactions
        notif_transactions=False,
        # loggin settings
        logging='info',
    )

    BrokerCls = None  # broker class will auto register
    DataCls = None  # data class will auto register

    # the granularity of the data. Must be one of ‘m1’, ‘m5’, ‘m15’, ‘m30’, ‘H1’, ‘H2’, ‘H3’, ‘H4’, ‘H6’, ‘H8’, ‘D1’, ‘W1’, or ‘M1’.
    _GRANULARITIES = {
        (bt.TimeFrame.Minutes, 1): 'M1',
        (bt.TimeFrame.Minutes, 5): 'M5',
        (bt.TimeFrame.Minutes, 15): 'M15',
        (bt.TimeFrame.Minutes, 30): 'M30',
        (bt.TimeFrame.Minutes, 60): 'H1',
        (bt.TimeFrame.Minutes, 120): 'H2',
        (bt.TimeFrame.Minutes, 180): 'H3',
        (bt.TimeFrame.Minutes, 240): 'H4',
        (bt.TimeFrame.Minutes, 360): 'H6',
        (bt.TimeFrame.Minutes, 480): 'H8',
        (bt.TimeFrame.Days, 1): 'D1',
        (bt.TimeFrame.Weeks, 1): 'W1',
        (bt.TimeFrame.Months, 1): 'M1',
    }

    _ORDEREXECS = {
        bt.Order.Market: 'MARKET',
        bt.Order.Limit: 'LIMIT',
        bt.Order.Stop: 'STOP',
        bt.Order.StopLimit: 'TAKE_PROFIT',
        bt.Order.StopTrail: 'TRAILING_STOP_LOSS'
    }

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        '''Initialization'''
        super(fxcmStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._cash = 0.0  # margin available, currently available cash
        self._value = 0.0  # account balance
        self._currency = None  # account currency
        self._leverage = 1  # leverage
        self._client_id_prefix = str(datetime.now().timestamp())

        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._env = None  # reference to cerebro for general notifications
        self._evt_acct = SerializableEvent()
        self._orders = collections.OrderedDict()  # map order.ref to order id
        self._trades = collections.OrderedDict()  # map order.ref to trade id

        self.fxapi = fxcmpy.fxcmpy(access_token=self.p.token, log_level=self.p.logging, server='demo', log_file='log.txt')



    def start(self, data=None, broker=None):
        # datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.streaming_events()
            self.broker_threads()

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_ordercreate.put(None)
            self.q_orderclose.put(None)
            self.q_account.put(None)

    def put_notification(self, msg, *args, **kwargs):
        '''Adds a notification'''
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    def get_positions(self):
        '''Returns the currently open positions'''
        try:
            response = self.fxapi.get_open_positions(kind='list')
            ## TODO: convert to dictionary with symbol as key
        except Exception as e:
            self.put_notification(self._create_error_notif(e, response))
        try:
            return response
        except NameError:
            return None

    def get_granularity(self, timeframe, compression):
        '''Returns the granularity useable for oanda'''
        return self._GRANULARITIES.get((timeframe, compression), None)

    def get_instrument(self, dataname):
        '''Returns details about the requested instrument'''
        try:
            response = self.fxapi.get_instruments()
            for idx in response:
                if idx == dataname:
                    inst[idx] = dataname
                    return inst[0]
            return {idx: 'Not available'}
        except Exception as e:
            self.put_notification(self._create_error_notif(e, response))

        try:
            return inst[0]
        except NameError:
            return None

    def get_instruments(self, dataname):
        '''Returns details about available instruments'''
        try:
            response = self.fxapi.get_instruments()
            for idx in response:
                inst[idx] = idx
        except Exception as e:
            self.put_notification(self._create_error_notif(e, response))

        try:
            return inst
        except NameError:
            return None

    def get_pricing(self, dataname):
        '''Returns details about current price'''
        try:
            ### TODO: must be subscribed
            response = self.fxapi.get_prices(dataname)
            # convert prices to dict
            for idx, val in enumerate(prices):
                prices[idx] = val.dict()
        except Exception as e:
            self.put_notification(self._create_error_notif(e, response))

        try:
            return prices[0]
        except NameError:
            return None


    def get_transactions_range(self, from_id, to_id, exclude_outer=False):
        '''Returns all transactions between range'''
        ## TODO
        return False

    def get_transactions_since(self, id):
        '''Returns all transactions since id'''
        ## TODO
        return False

    def get_cash(self):
        '''Returns the available cash'''
        return self._cash

    def get_value(self):
        '''Returns the account balance'''
        return self._value

    def get_currency(self):
        '''Returns the currency of the account'''
        return self._currency

    def get_leverage(self):
        '''Returns the leverage of the account'''
        return self._leverage

    def broker_threads(self):
        '''Creates threads for broker functionality'''
        self.q_account = queue.Queue()
        self.q_account.put(True)  # force an immediate update
        t = threading.Thread(target=self._t_account)
        t.daemon = True
        t.start()

        self.q_ordercreate = queue.Queue()
        t = threading.Thread(target=self._t_order_create)
        t.daemon = True
        t.start()

        self.q_orderclose = queue.Queue()
        t = threading.Thread(target=self._t_order_cancel)
        t.daemon = True
        t.start()

        # Wait once for the values to be set
        self._evt_acct.wait(self.p.account_poll_freq)

    def streaming_events(self):
        '''Creates threads for event streaming'''
        q = queue.Queue()
        kwargs = {'q': q}
        t = threading.Thread(target=self._t_streaming_events, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def streaming_prices(self, dataname):
        '''Creates threads for price streaming'''
        q = queue.Queue()
        kwargs = {'q': q, 'dataname': dataname}
        t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def order_create(self, order, stopside=None, takeside=None, **kwargs):
        '''Creates an order'''
        okwargs = dict()
        okwargs['instrument'] = order.data._dataname
        okwargs['units'] = (
            abs(int(order.created.size)) if order.isbuy()
            else -abs(int(order.created.size)))  # negative for selling
        okwargs['type'] = self._ORDEREXECS[order.exectype]
        okwargs['replace'] = order.info.get("replace", None)

        if order.exectype != bt.Order.Market:
            okwargs['price'] = format(
                order.created.price,
                '.%df' % order.data.contractdetails['displayPrecision'])
            if order.valid is None:
                okwargs['timeInForce'] = 'GTC'  # good to cancel
            else:
                okwargs['timeInForce'] = 'GTD'  # good to date
                gtdtime = order.data.num2date(order.valid)
                okwargs['gtdTime'] = gtdtime.strftime(self._DATE_FORMAT)

        if order.exectype == bt.Order.StopLimit:
            if "replace" not in okwargs:
                raise Exception("replace param needed for StopLimit order")
            okwargs['price'] = format(
                order.plimit or order.price,
                '.%df' % order.data.contractdetails['displayPrecision'])

        if order.exectype == bt.Order.StopTrail:
            if "replace" not in okwargs:
                raise Exception("replace param needed for StopTrail order")
            trailamount = order.trailamount
            if order.trailpercent:
                trailamount = order.price * order.trailpercent
            okwargs['distance'] = format(
                trailamount,
                '.%df' % order.data.contractdetails['displayPrecision'])

        ### TODO

        okwargs.update(**kwargs)  # anything from the user
        self.q_ordercreate.put((order.ref, okwargs,))

        # notify orders of being submitted
        self.broker._submit(order.ref)
        if stopside is not None:  # don't make price on stopside mandatory
            self.broker._submit(stopside.ref)
        if takeside is not None and takeside.price is not None:
            self.broker._submit(takeside.ref)

        return order

    def order_cancel(self, order):
        '''Cancels a order'''
        self.q_orderclose.put(order.ref)
        return order

    def candles(self, dataname, dtbegin, dtend, timeframe, compression,
                candleFormat, includeFirst=True, onlyComplete=True):
        '''Returns historical rates'''
        q = queue.Queue()
        kwargs = {'dataname': dataname, 'dtbegin': dtbegin, 'dtend': dtend,
                  'timeframe': timeframe, 'compression': compression,
                  'candleFormat': candleFormat, 'includeFirst': includeFirst,
                  'onlyComplete': onlyComplete, 'q': q}
        t = threading.Thread(target=self._t_candles, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _oref_to_client_id(self, oref):
        '''Converts a oref to client id'''
        id = "{}-{}".format(self._client_id_prefix, oref)
        return id

    def _client_id_to_oref(self, client_id):
        '''Converts a client id to oref'''
        oref = None
        if str(client_id).startswith(self._client_id_prefix):
            oref = int(str(client_id)[len(self._client_id_prefix)+1:])
        return oref

    def _t_account(self):
        '''Callback method for account request'''
        while True:
            try:
                msg = self.q_account.get(timeout=self.p.account_poll_freq)
                if msg is None:
                    break  # end of thread
            except queue.Empty:  # tmout -> time to refresh
                pass

            try:
                accinfo = self.fxcmapi.get_accounts_summary(kind='list')
            except Exception as e:
                self.put_notification(
                    self._create_error_notif(
                        e,
                        response))
                return

            ### TODO: list is returned
            try:
                self._cash = accinfo[0]['usableMargin']
                self._value = accinfo[0]['balance']
                self._currency = 'EUR' #accinfo.currency
                self._leverage = 1/accinfo[0]['usableMargin']
            except KeyError:
                pass

            # notify of success, initialization waits for it
            self._evt_acct.set()

    def _t_streaming_events(self, q):
        '''Callback method for streaming events'''
        last_id = None
        reconnections = 0
        while True:
            try:
                response = self.oapi_stream.transaction.stream(
                    self.p.account
                )
                # process response
                for msg_type, msg in response.parts():
                    # if a connection occurred
                    if reconnections > 0:
                        if reconnections > 0 and last_id:
                            # get all transactions between the last seen and first from
                            # reconnected stream
                            old_transactions = self.get_transactions_since(
                                last_id)
                            for t in old_transactions:
                                if msg_type == "transaction.Transaction":
                                    if t.id > last_id:
                                        self._transaction(t.dict())
                                        last_id = t.id
                        reconnections = 0
                    if msg_type == "transaction.Transaction":
                        if not last_id or msg.id > last_id:
                            self._transaction(msg.dict())
                            last_id = msg.id
            except Exception as e:
                self.put_notification(
                    self._create_error_notif(
                        e,
                        response))

    def _t_streaming_prices(self, dataname, q):
        '''Callback method for streaming prices'''
        try:
            response = self.oapi_stream.pricing.stream(
                self.p.account,
                instruments=dataname,
            )
            # process response
            for msg_type, msg in response.parts():
                if msg_type == "pricing.ClientPrice":
                    # put price into queue as dict
                    q.put(msg.dict())
        except Exception as e:
            self.put_notification(
                self._create_error_notif(
                    e,
                    response))

    def _t_candles(self, dataname, dtbegin, dtend, timeframe, compression,
                   candleFormat, includeFirst, onlyComplete, q):
        '''Callback method for candles request'''
        granularity = self.get_granularity(timeframe, compression)
        if granularity is None:
            q.put(None)
            return

        dtkwargs = {}
        if dtbegin is not None:
            dtkwargs['fromTime'] = dtbegin.strftime(self._DATE_FORMAT)
            dtkwargs['includeFirst'] = includeFirst

        count = 0
        reconnections = 0
        while True:
            if count > 1:
                dtkwargs['includeFirst'] = False
            try:
                response = self.oapi.instrument.candles(
                    dataname,
                    granularity=granularity,
                    price=candleFormat,
                    **dtkwargs)
                candles = response.get('candles', 200)
                reconnections = 0
                count += 1
            except Exception as e:
                self.put_notification(
                    self._create_error_notif(
                        e,
                        response))
                continue

            dtobj = None
            for candle in candles:
                # get current candle time
                dtobj = datetime.utcfromtimestamp(float(candle.time))
                # if end time is provided, check if time is reached for
                # every candle
                if dtend is not None and dtobj > dtend:
                    break
                # add candle
                if not onlyComplete or candle.complete:
                    q.put(candle.dict())

            if dtobj is not None:
                dtkwargs['fromTime'] = dtobj.strftime(self._DATE_FORMAT)
            elif dtobj is None:
                break
            if dtend is not None and dtobj > dtend:
                break
            if len(candles) == 0:
                break

        q.put({})  # end of transmission'''

    def _transaction(self, trans):
        if self.p.notif_transactions:
            self.put_notification(str(trans))
        oid = None
        ttype = trans['type']

        if ttype in self._X_CREATE_TRANS:
            # get order id (matches transaction id)
            oid = trans['id']
            oref = None
            # identify backtrader order by checking client
            # extensions (this is set when creating a order)
            if 'clientExtensions' in trans:
                # assume backtrader created the order for this transaction
                oref = self._client_id_to_oref(trans['clientExtensions']['id'])
            if oref is not None:
                self._orders[oid] = oref

        elif ttype in self._X_FILL_TRANS:
            # order was filled, notify backtrader of it
            oid = trans['orderID']

        elif ttype in self._X_CANCEL_TRANS:
            # order was cancelled, notify backtrader of it
            oid = trans['orderID']

        elif ttype in self._X_REJECT_TRANS:
            # transaction was rejected, notify backtrader of it
            oid = trans['requestID']

        elif ttype in self._X_IGNORE_TRANS:
            # transaction can be ignored
            msg = 'Received transaction {} with id {}. Ignoring transaction.'
            msg = msg.format(ttype, trans['id'])
            self.put_notification(msg, trans)

        else:
            msg = 'Received transaction {} with id {}. Unknown situation.'
            msg = msg.format(ttype, trans['id'])
            self.put_notification(msg, trans)
            return

        if oid in self._orders:
            # when an order id exists process transaction
            self._process_transaction(oid, trans)
            self._process_trades(self._orders[oid], trans)
        else:
            # external order created this transaction
            if self.broker.p.use_positions and ttype in self._X_FILL_TRANS:
                size = float(trans['units'])
                price = float(trans['price'])
                for data in self.datas:
                    if data._name == trans['instrument']:
                        self.broker._fill_external(data, size, price)
                        break
            elif ttype not in self._X_IGNORE_TRANS:
                # notify about unknown transaction
                if self.broker.p.use_positions:
                    msg = 'Received external transaction {} with id {}. Skipping transaction.'
                else:
                    msg = 'Received external transaction {} with id {}. Positions and trades may not match anymore.'
                msg = msg.format(ttype, trans['id'])
                self.put_notification(msg, trans)

    def _process_transaction(self, oid, trans):
        try:
            # get a reference to a backtrader order based on
            # the order id / trade id
            oref = self._orders[oid]
        except KeyError:
            return

        ttype = trans['type']
        if ttype in self._X_CREATE_TRANS:
            self.broker._accept(oref)

        elif ttype in self._X_FILL_TRANS:
            size = float(trans['units'])
            price = float(trans['price'])
            self.broker._fill(oref, size, price, reason=trans['reason'])
            # store order ids which generated by the order
            if 'tradeOpened' in trans:
                self._orders[trans['tradeOpened']['tradeID']] = oref
            if 'tradeReduced' in trans:
                self._orders[trans['tradeReduced']['tradeID']] = oref

        elif ttype in self._X_CANCEL_TRANS:
            reason = trans['reason']
            if reason == 'TIME_IN_FORCE_EXPIRED':
                self.broker._expire(oref)
            else:
                self.broker._cancel(oref)

        elif ttype in self._X_REJECT_TRANS:
            self.broker._reject(oref)

    def _process_trades(self, oref, trans):
        if 'tradeID' in trans:
            self._trades[oref] = trans['tradeID']
        if 'tradeOpened' in trans:
            self._trades[oref] = trans['tradeOpened']['tradeID']
        if 'tradeClosed' in trans:
            self._trades[oref] = trans['tradeClosed']['tradeID']
        if 'tradesClosed' in trans:
            for t in trans['tradesClosed']:
                for key, value in self._trades.copy().items():
                    if value == t['tradeID']:
                        del self._trades[key]

    def _t_order_create(self):
        while True:
            msg = self.q_ordercreate.get()
            if msg is None:
                break

            oref, okwargs = msg
            try:
                if okwargs["replace"]:
                    oid = "@{}".format(self._oref_to_client_id(okwargs["replace"]))
                    if okwargs["replace"] in self._trades:
                        okwargs["tradeID"] = self._trades[okwargs["replace"]]
                    response = self.oapi.order.replace(
                        self.p.account,
                        oid,
                        order=okwargs)
                else:
                    response = self.oapi.order.create(
                        self.p.account,
                        order=okwargs)
                # get the transaction which created the order
                o = response.get("orderCreateTransaction", 201)
            except Exception as e:
                self.put_notification(
                    self._create_error_notif(
                        e,
                        response))
                self.broker._reject(oref)
                continue

    def _t_order_cancel(self):
        while True:
            oref = self.q_orderclose.get()
            if oref is None:
                break

            oid = None
            for key, value in self._orders.items():
                if value == oref:
                    oid = key
                    break

            if oid is None:
                continue  # the order is no longer there
            try:
                # TODO either close pending orders or filled trades
                response = self.oapi.order.cancel(self.p.account, oid)
            except Exception as e:
                self.put_notification(
                    self._create_error_notif(
                        e,
                        response))
                continue

            self.broker._cancel(oref)

    def _create_error_notif(self, e, response):
        try:
            notif = "{}: {} - {}".format(
                response.status,
                response.reason,
                response.get("errorMessage"))
        except Exception:
            notif = str(e)
        return notif


if __name__ == '__main__':
    s = fxcmStore()
