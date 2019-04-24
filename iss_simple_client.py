#!/usr/bin/env python
"""
    Small example of a library implementing interaction with Moscow Exchange ISS server.

    Version: 1.2
    Developed for Python 2.6

    @copyright: 2016 by MOEX
"""

import urllib.request, urllib.error, urllib.parse
import base64
import http.cookiejar
import json
import time


requests = {'history_secs': 'http://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s',
            'sec_trades': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities/%(sec)s/trades.json?previous_session=%(previous_session)d&limit=%(limit)d&reversed=%(reversed)d',
            'sec_trades1': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities/%(sec)s/trades.json?previous_session=%(previous_session)d&tradeno=%(tradeno)d&limit=%(limit)d',
	    'sec_candleborders': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(sec)s/candleborders.json',
            'sec_candles': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(sec)s/candles.json?start=%(start)d&till=%(till)s&from=%(from)s&interval=%(interval)d&iss.reverse=%(reverse)s' }
# futures, forts, RTSI, RIH8
# http://iss.moex.com/iss/securities.xml?q=RI
timeFrameCodes = { 'm1': 1, 'm10': 10, 'H1': 60, 'D1': 24, 'W1': 7, 'M1': 31, 'Q1': 4 }

class Config:
    def __init__(self, user='', password='', proxy_url='', debug_level=0):
        """ Container for all the configuration options:
            user: username in MOEX Passport to access real-time data and history
            password: password for this user
            proxy_url: proxy URL if any is used, specified as http://proxy:port
            debug_level: 0 - no output, 1 - send debug info to stdout
        """
        self.debug_level = debug_level  
        self.proxy_url = proxy_url
        self.user = user
        self.password = password
        self.auth_url = "https://passport.moex.com/authenticate"


class MicexAuth:
    """ user authentication data and functions
    """

    def __init__(self, config):
        self.config = config
        self.cookie_jar = http.cookiejar.CookieJar()
        if config.user != '':
            self.auth()

    def auth(self):
        """ one attempt to authenticate
        """
        # opener for https authorization
        if self.config.proxy_url:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": self.config.proxy_url}),
                                          urllib.request.HTTPCookieProcessor(self.cookie_jar),
                                          urllib.request.HTTPHandler(debuglevel=self.config.debug_level))
        else:
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar),
                                          urllib.request.HTTPHandler(debuglevel=self.config.debug_level))
        base64_str = base64.encodestring((self.config.user + ':' + self.config.password).encode()).decode().replace('\n', '')
        opener.addheaders = [('Authorization',
                              'Basic %s' % base64_str)]
        #opener.addheaders = [('Authorization',
        #                      'Basic %s' % base64.encodestring(self.config.user + ':' + self.config.password)[:-1])]
        get_cert = opener.open(self.config.auth_url)

        # we only need a cookie with MOEX Passport (certificate)
        self.passport = None
        for cookie in self.cookie_jar:
            if cookie.name == 'MicexPassportCert':
                self.passport = cookie
                break
        if self.passport is None:
            print("Cookie not found!")

    def is_real_time(self):
        if self.config.user == '':
            return False
        """ repeat auth request if failed last time or cookie expired
        """
        if not self.passport or (self.passport and self.passport.is_expired()):
            self.auth()
        if self.passport and not self.passport.is_expired():
            return True
        return False


class MicexISSDataHandler:
    """ Data handler which will be called
    by the ISS client to handle downloaded data.
    """
    def __init__(self, container):
        """ The handler will have a container to store received data.
        """
        self.data = container()

    def do(self):
        """ This handler method should be overridden to perform
        the processing of data returned by the server.
        """
        pass

class MicexISSClient:
    """ Methods for interacting with the MICEX ISS server.
    """

    def __init__( self, config, handler, container, **kwargs ):
        """ Create opener for a connection with authorization cookie.
        It's not possible to reuse the opener used to authenticate because
        there's no method in opener to remove auth data.
            config: instance of the Config class with configuration options
            auth: instance of the MicexAuth class with authentication info
            handler: user's handler class inherited from MicexISSDataHandler
            containet: user's container class
        """
        auth = kwargs['auth'] if 'auth' in kwargs else None
        if auth != None:
            if config.proxy_url:
                self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": config.proxy_url}),
                                                   urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                                                   urllib.request.HTTPHandler(debuglevel=config.debug_level))
            else:
                self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                                                   urllib.request.HTTPHandler(debuglevel=config.debug_level))
        else:
            if config.proxy_url:
                self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": config.proxy_url}),
                                                   urllib.request.HTTPHandler(debuglevel=config.debug_level))
            else:
                self.opener = urllib.request.build_opener( urllib.request.HTTPHandler(debuglevel=config.debug_level) )
        urllib.request.install_opener(self.opener)
        self.handler = handler(container)

    def get_history_securities(self, engine, market, board, date):
        """ Get and parse historical data on all the securities at the
        given engine, market, board
        """
        url = requests['history_secs'] % {'engine': engine,
                                          'market': market,
                                          'board': board,
                                          'date': date}

        # always remember about the 'start' argument to get long replies
        start = 0
        cnt = 1
        while cnt > 0:
            res = self.opener.open( url + '&start=' + str(start) )
            #resStr = str( res.read().decode('utf-8') )
            print( res )
            #if resStr == '':
            #    break
            #jres = json.load(res)
            jres = json.loads( res.read().decode('utf-8') )

            # the following is also just a simple example
            # it is recommended to keep metadata separately

            # root node with historical data
            jhist = jres['history']

            # node with actual data
            jdata = jhist['data']

            # node with the list of column IDs in 'data' in correct order;
            # it's also possible to use the iss.json=extended argument instead
            # to get all the IDs together with data (leads to more traffic)
            jcols = jhist['columns']
            secIdx = jcols.index('SECID')
            closeIdx = jcols.index('LEGALCLOSEPRICE')
            tradesIdx = jcols.index('NUMTRADES')

            result = []
            for sec in jdata:
                result.append((sec[secIdx],
                               del_null(sec[closeIdx]),
                               del_null(sec[tradesIdx])))
            # we return pieces of received data on each iteration
            # in order to be able to handle large volumes of data
            # and to start data processing without waiting for
            # the complete reply
            self.handler.do(result)
            cnt = len(jdata)
            start = start + cnt
        return True

    def get_security_trades( self, engine, market, security, prevSession, isReversed, limit ):
        """ Get and parse historical data on all the securities at the
        given engine, market, board
        """
        url = requests['sec_trades'] % {'engine': engine,
                                        'market': market,
                                        'sec': security,
                                        'previous_session': prevSession,
                                        'limit': limit }

        print(url)

        # always remember about the 'start' argument to get long replies

        res = self.opener.open( url )
        resStr = str( res.read().decode('utf-8') )
        #print( resStr )
        #if resStr == '':
        #    break
        #jres = json.load(res)
        jres = json.loads( resStr )

        # the following is also just a simple example
        # it is recommended to keep metadata separately

        # root node with historical data
        # see json response structure here:
        # https://iss.moex.com/iss/engines/futures/markets/forts/securities/RIH8/trades.json?reversed=1&limit=10
            
        jhist = jres['trades']

        # node with actual data
        jdata = jhist['data']

        # node with the list of column IDs in 'data' in correct order;
        # it's also possible to use the iss.json=extended argument instead
        # to get all the IDs together with data (leads to more traffic)
        jcols = jhist['columns']
        timeIdx = jcols.index('SYSTIME')
        priceIdx = jcols.index('PRICE')
        qtyIdx = jcols.index('QUANTITY')
            

        result = []
        for trade in jdata:
            result.append((trade[timeIdx],
                            del_null(trade[priceIdx]),
                            del_null(trade[qtyIdx])))
        # we return pieces of received data on each iteration
        # in order to be able to handle large volumes of data
        # and to start data processing without waiting for
        # the complete reply
        self.handler.do( result )
        
        return True

    def get_session_start_end_tradenos( self, engine, market, security, prevSession ):
        url = requests['sec_trades'] % {'engine': engine,
                                        'market': market,
                                        'sec': security,
                                        'previous_session': prevSession,
                                        'reversed': 0,
                                        'limit': 1 }

        #print( url )
        res = self.opener.open( url )
        resStr = str( res.read().decode('utf-8') )
        jres = json.loads( resStr )

        # the following is also just a simple example
        # it is recommended to keep metadata separately
            
        jhist = jres['trades']
        jdata = jhist['data']

        if len( jdata ) == 0:
            raise ValueError( 'Can\'t get session start tradeno' )
        
        jcols = jhist['columns']
            
        sessionStartTradeNo = int( del_null( jdata[0][ jcols.index( 'TRADENO' ) ] ) )

        print( 'session start time:', jdata[0][ jcols.index( 'SYSTIME' ) ] )

        if prevSession == 0:
            url = requests['sec_trades'] % {'engine': engine,
                                            'market': market,
                                            'sec': security,
                                            'previous_session': prevSession,
                                            'reversed': 1,
                                            'limit': 1 }

            res = self.opener.open( url )
            resStr = str( res.read().decode('utf-8') )
            jres = json.loads( resStr )

            # the following is also just a simple example
            # it is recommended to keep metadata separately
                    
            jhist = jres['trades']
            jdata = jhist['data']

            if len( jdata ) == 0:
                raise ValueError( 'Can\'t get session end tradeno' )
                
            jcols = jhist['columns']

            sessionEndTradeNo = int( del_null( jdata[0][ jcols.index( 'TRADENO' ) ] ) )

            print( 'session end time:', jdata[0][ jcols.index( 'SYSTIME' ) ] )
        else:
            url = requests['sec_trades'] % {'engine': engine,
                                            'market': market,
                                            'sec': security,
                                            'previous_session': ( prevSession - 1 ),
                                            'reversed': 1,
                                            'limit': 1 }

            res = self.opener.open( url )
            resStr = str( res.read().decode('utf-8') )
            jres = json.loads( resStr )

            # the following is also just a simple example
            # it is recommended to keep metadata separately
                    
            jhist = jres['trades']
            jdata = jhist['data']

            if len( jdata ) == 0:
                raise ValueError( 'Can\'t get session end tradeno' )
                
            jcols = jhist['columns']

            sessionEndTradeNo = int( del_null( jdata[0][ jcols.index( 'TRADENO' ) ] ) ) - 1
        
        return ( sessionStartTradeNo, sessionEndTradeNo )
        

    # prev_session = 0 means the current session
    # -----||----- = n means number of the previous session before current one
    def get_trades_for_session( self, engine, market, security, prevSession ):
        startTradeNo, endTradeNo = self.get_session_start_end_tradenos( engine, market, security, prevSession )

        result = []
        currTradeNo = startTradeNo
        while currTradeNo <= endTradeNo:
            print( currTradeNo )
            url = requests['sec_trades1'] % {'engine': engine,
                                                'market': market,
                                                'sec': security,
                                                'previous_session': prevSession,
                                                'tradeno': currTradeNo,
                                                'limit': 5000 }


            # always remember about the 'start' argument to get long replies

            res = self.opener.open( url )
            resStr = str( res.read().decode('utf-8') )
            #print( resStr )
            #if resStr == '':
            #    break
            #jres = json.load(res)
            jres = json.loads( resStr )

            # the following is also just a simple example
            # it is recommended to keep metadata separately

            # root node with historical data
            # see json response structure here:
            # https://iss.moex.com/iss/engines/futures/markets/forts/securities/RIH8/trades.json?reversed=1&limit=10
                
            jhist = jres['trades']

            # node with actual data
            jdata = jhist['data']

            if len( jdata ) == 0:
                break

            # node with the list of column IDs in 'data' in correct order;
            # it's also possible to use the iss.json=extended argument instead
            # to get all the IDs together with data (leads to more traffic)
            jcols = jhist['columns']
            timeIdx = jcols.index('SYSTIME')
            priceIdx = jcols.index('PRICE')
            qtyIdx = jcols.index('QUANTITY')
            tradenoIdx = jcols.index( 'TRADENO' )    

            chunk = []
            for trade in jdata:
                dtime = time.strptime( trade[timeIdx] + '+0300', '%Y-%m-%d %H:%M:%S%z' )
                timeEpoch = int( time.mktime( dtime ) )
                chunk.append( ( timeEpoch,
                                float( del_null( trade[priceIdx] ) ),
                                int( del_null( trade[qtyIdx] ) ),
                                int( del_null( trade[tradenoIdx] ) ) ) )
            # we return pieces of received data on each iteration
            # in order to be able to handle large volumes of data
            # and to start data processing without waiting for
            # the complete reply
            result += chunk

            currTradeNo = int( del_null( jdata[-1][ tradenoIdx ] ) ) + 1
            
        self.handler.do( result )
        
        return True

    def get_security_candleborders( self, engine, market, board, security, timeFrames ):
        """ Get and parse historical data on all the securities at the
        given engine, market, board
        """
        url = requests['sec_candleborders'] % {'engine': engine,
                                               'market': market,
                                               'board': board,
                                               'sec': security }

        #print(url)

        res = self.opener.open( url )
        resStr = str( res.read().decode('utf-8') )
        #print( resStr )

        jres = json.loads( resStr )

        jdata_raw = jres['borders']
        # node with actual data
        jcols = jdata_raw['columns']
        jdata = jdata_raw['data']

        # node with the list of column IDs in 'data' in correct order;
        # it's also possible to use the iss.json=extended argument instead
        # to get all the IDs together with data (leads to more traffic)
        #"begin", "end", "interval"
        beginIdx = jcols.index('begin')
        endIdx = jcols.index('end')
        intervalIdx = jcols.index('interval')
            

        result = {}
        _timeFrames = list( timeFrames )
        for timeFrameData in jdata:
            i = 0
            while i < len( _timeFrames ):
                if timeFrameData[ intervalIdx ] == timeFrameCodes[ _timeFrames[i] ]:
                    result[ _timeFrames[i] ] = ( timeFrameData[ beginIdx ], timeFrameData[ endIdx ] )
                    _timeFrames.pop(i)
                    continue
                i += 1
        
        return result

    def get_security_candles( self, engine, market, board, security, dateFrom, dateTill, timeFrame, reverse = False ):
 
        candlesRead = 0
        candles = []

        reqNo = 0
        while True:
            if reqNo % 10 == 0:
                print( reqNo, end = ' ' )
            url = requests['sec_candles'] % {'engine': engine,
                                             'market': market,
                                             'board': board,
                                             'sec': security,
                                             'till': dateTill,
                                             'from': dateFrom,
                                             'interval': timeFrameCodes[ timeFrame ],
                                             'reverse': reverse,
                                             'start': candlesRead }

            reqNo += 1
            #print(url)

            res = self.opener.open( url )
            resStr = str( res.read().decode('utf-8') )
            #print( resStr )

            jres = json.loads( resStr )

            jdata_raw = jres['candles']
            # node with actual data
            jcols = jdata_raw['columns']
            jdata = jdata_raw['data']

            # node with the list of column IDs in 'data' in correct order;
            # it's also possible to use the iss.json=extended argument instead
            # to get all the IDs together with data (leads to more traffic)
            #"open", "close", "high", "low", "value", "volume", "begin", "end"
            openIdx = jcols.index('open')
            closeIdx = jcols.index('close')
            highIdx = jcols.index('high')
            lowIdx = jcols.index('low')
            valueIdx = jcols.index('value')
            volumeIdx = jcols.index('volume')
            beginIdx = jcols.index('begin')
            endIdx = jcols.index('end')

            dataChunk = []
            for cd in jdata:
                dataChunk.append( [ cd[ openIdx ], cd[ closeIdx ], cd[ highIdx ], cd[ lowIdx ],
                                    cd[ valueIdx ], cd[ volumeIdx ], cd[ beginIdx ], cd[ endIdx ] ] )

            if len( dataChunk ) == 0:
                break
            
            candles += dataChunk
            candlesRead += len( dataChunk )
        print( '\n' )
        
        return candles

    def save_security_candles( self, engine, market, board, security, timeFrame, **kwargs ):
        if 'time_bounds' not in kwargs:
            limits = self.get_security_candleborders( engine, market, board, security, ( 'm1', ) )

            if 'm1' not in limits:
                return

            dateFrom = limits[ 'm1' ][0]
            dateTill = limits[ 'm1' ][1]
            dateFrom = dateFrom[ :dateFrom.find( ' ' ) ]
            dateTill = dateTill[ :dateTill.find( ' ' ) ]
        else:
            dateFrom = kwargs[ 'time_bounds' ][ 0 ]
            dateTill = kwargs[ 'time_bounds' ][ 1 ]

        fnameOut = '%s.%s.%s.%s.txt' % ( security,
                                     dateFrom,
                                     dateTill,
                                     timeFrame )

        #print( fnameOut )

        f = open( fnameOut, 'w' )

        candlesRead = 0
        #candles = []

        reqNo = 0
        while True:
            if reqNo % 10 == 0:
                print( reqNo, end = ' ' )
            url = requests['sec_candles'] % {'engine': engine,
                                             'market': market,
                                             'board': board,
                                             'sec': security,
                                             'till': dateTill,
                                             'from': dateFrom,
                                             'interval': timeFrameCodes[ timeFrame ],
                                             'reverse': False,
                                             'start': candlesRead }

            reqNo += 1
            #print(url)

            res = self.opener.open( url )
            resStr = str( res.read().decode('utf-8') )
            #print( resStr )

            jres = json.loads( resStr )

            jdata_raw = jres['candles']
            # node with actual data
            jcols = jdata_raw['columns']
            jdata = jdata_raw['data']

            # node with the list of column IDs in 'data' in correct order;
            # it's also possible to use the iss.json=extended argument instead
            # to get all the IDs together with data (leads to more traffic)
            #"open", "close", "high", "low", "value", "volume", "begin", "end"
            openIdx = jcols.index('open')
            closeIdx = jcols.index('close')
            highIdx = jcols.index('high')
            lowIdx = jcols.index('low')
            valueIdx = jcols.index('value')
            volumeIdx = jcols.index('volume')
            beginIdx = jcols.index('begin')
            endIdx = jcols.index('end')

            #dataChunk = []
            dataChunkSize = 0
            for cd in jdata:
                #dataChunk.append( [ cd[ openIdx ], cd[ closeIdx ], cd[ highIdx ], cd[ lowIdx ],
                #                    cd[ valueIdx ], cd[ volumeIdx ], cd[ beginIdx ], cd[ endIdx ] ] )
                f.write( '%f\t%f\t%f\t%f\t%f\t%f\t%s\t%s\n' % ( cd[ openIdx ], cd[ closeIdx ], cd[ highIdx ], cd[ lowIdx ],
                                                                cd[ valueIdx ], cd[ volumeIdx ], cd[ beginIdx ], cd[ endIdx ] ) )
                dataChunkSize += 1

            #if len( dataChunk ) == 0:
            #    break
            if dataChunkSize == 0:
                break
            
            #candles += dataChunk
            #candlesRead += len( dataChunk )
            candlesRead += dataChunkSize
        print( '\n' )

        f.close()
        
        #print( candles )


def del_null(num):
    """ replace null string with zero
    """
    return 0 if num is None else num


if __name__ == '__main__':
    pass
