#!/usr/bin/env python
"""
    Small example of interaction with Moscow Exchange ISS server.

    Version: 1.1
    Developed for Python 2.6

    Requires iss_simple_client.py library.
    Note that the valid username and password for the MOEX ISS account
    are required in order to perform the given request for historical data.

    @copyright: 2016 by MOEX
"""

import sys, time
from iss_simple_client import Config
from iss_simple_client import MicexAuth
from iss_simple_client import MicexISSClient
from iss_simple_client import MicexISSDataHandler

SEC = 'RIH8'

class MyData:
    """ Container that will be used by the handler to store data.
    Kept separately from the handler for scalability purposes: in order
    to differentiate storage and output from the processing.
    """
    def __init__(self):
        self.history = []

    def print_history(self):
        print("=" * 49)
        print("|%15s|%15s|%15s|" % ("SECID", "CLOSE", "TRADES"))
        print("=" * 49)
        for sec in self.history:
            print("|%15s|%15.2f|%15d|" % (sec[0], sec[1], sec[2]))
        print("=" * 49)

    def print_trades( self ):
        print("=" * 49)
        print("|%15s|%15s|%15s|" % ("SYSTIME", "PRICE", "QUANTITY"))
        print("=" * 49)
        for sec in self.history:
            print("|%15s|%15.2f|%15d|" % ( time.strftime( '%Y-%m-%d %H:%M:%S%z', time.localtime( sec[0] ) ),
                                          sec[1], sec[2]) )
        print("=" * 49)


    def store_trades( self ):
        zeroTime = self.history[0][0]
        endTime = self.history[-1][0]

        print( time.strftime( '%Y-%m-%d %H:%M:%S%z', time.localtime( zeroTime ) ) )
        print( time.strftime( '%Y-%m-%d %H:%M:%S%z', time.localtime( endTime ) ) )
        
        fname = '%s %s.txt' % ( SEC, time.strftime( '%d%m%y %H-%M-%S%z', time.localtime( zeroTime ) ) )
        #removing ':' from name
        fname = fname.translate( str.maketrans( '', '', ':' ) )

        f = open( fname, 'w' )
        f.write( 'zeroTime: %d\n' % zeroTime )
        for sec in self.history:
            f.write( '%d\t%.0f\t%d\n' % ( sec[0] - zeroTime, sec[1], sec[2] ) )
        f.close()



class MyDataHandler(MicexISSDataHandler):
    """ This handler will be receiving pieces of data from the ISS client.
    """
    def do(self, market_data):
        """ Just as an example we add all the chunks to one list.
        In real application other options should be considered because some
        server replies may be too big to be kept in memory.
        """
        self.data.history = self.data.history + market_data


def main():
    my_config = Config( user='', password='', proxy_url='' )
    my_auth = MicexAuth(my_config)
    if my_auth.is_real_time():
        iss = MicexISSClient(my_config, MyDataHandler, MyData, auth = my_auth )
    else:
        iss = MicexISSClient(my_config, MyDataHandler, MyData, auth = None )

    """
    iss.get_history_securities('futures', # see http://iss.moex.com/iss/engines.xml
                                'index', # see http://iss.moex.com/iss/engines/stock/markets.xml
                                'RTSI', # http://iss.moex.com/iss/engines/stock/markets/index/boards.xml
                                '2018-01-19'
                               )
    iss.handler.data.print_history()
    """
    """
    iss.get_security_trades('futures', # see http://iss.moex.com/iss/engines.xml
                            'forts', # see http://iss.moex.com/iss/engines/stock/markets.xml
                            'RIH8', # http://iss.moex.com/iss/engines/stock/markets/index/boards.xml
                            False,
                            10
                            )
    """
    """
	#print( iss.get_session_start_end_tradenos( 'futures', 'forts', 'RIH8', 1 ) )
    #iss.handler.data.print_trades()
	iss.get_trades_for_session( 'futures', 'forts', 'RIH8', 3 )
    iss.handler.data.store_trades()
    """
    #   To find a pass to a given secutity lool at the following links:
    #	https://iss.moex.com/iss/engines/
    #	http://iss.moex.com/iss/engines/currency/markets
    #	https://iss.moex.com/iss/engines/currency/markets/selt/boards
    #	https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities
    #	now the request is fulfilled:
    #   https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000000TOD/candleborders.json
    #   last argument is a list of timeframes for which the query to be done
    #   timeframe text code is one of: ( 'm1', 'm10', 'H1', 'D1', 'W1', 'M1', 'Q1' )

    #borders = iss.get_security_candleborders( 'currency', 'selt', 'CETS', 'USD000000TOD', ( 'D1', 'H1', ) )
    #print( borders )
    #candles = iss.get_security_candles( 'currency', 'selt', 'CETS', 'USD000000TOD', '2010-04-10', '', 'm1' )
    iss.save_security_candles( 'currency', 'selt', 'CETS', 'USD000000TOD', 'H1', time_bounds = ( '2011-12-15', '2019-04-01' ) )

    #print( candles )
    
    #else:
    #    print( 'not real time' )

if __name__ == '__main__':
    #try:
    main()
    #except:
    #    print("Sorry:", sys.exc_info()[0], ":", sys.exc_info()[1])
