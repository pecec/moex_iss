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

import sys
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
            print("|%15s|%15.2f|%15d|" % (sec[0], sec[1], sec[2]))
        print("=" * 49)

        fname = '%s %s.txt' % ( SEC, self.history[-1][0] )
        #removing ':' from name
        fname = fname.translate( str.maketrans( '', '', ':' ) )

        f = open( fname, 'w' )
        for sec in self.history:
            f.write( '%s\t%s\t%s\n' % (sec[0], sec[1], sec[2]) )
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
    iss.get_security_trades('futures', # see http://iss.moex.com/iss/engines.xml
                            'forts', # see http://iss.moex.com/iss/engines/stock/markets.xml
                            'RIH8', # http://iss.moex.com/iss/engines/stock/markets/index/boards.xml
                            True,
                            200
                            )
    iss.handler.data.print_trades()
    
    #else:
    #    print( 'not real time' )

if __name__ == '__main__':
    #try:
    main()
    #except:
    #    print("Sorry:", sys.exc_info()[0], ":", sys.exc_info()[1])
