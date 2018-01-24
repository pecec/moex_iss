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


class MyData:
    """ Container that will be used by the handler to store data.
    Kept separately from the handler for scalability purposes: in order
    to differentiate storage and output from the processing.
    """
    def __init__(self):
        self.history = []

    def print_history(self):
        print "=" * 49
        print "|%15s|%15s|%15s|" % ("SECID", "CLOSE", "TRADES")
        print "=" * 49
        for sec in self.history:
            print "|%15s|%15.2f|%15d|" % (sec[0], sec[1], sec[2])
        print "=" * 49


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
    my_config = Config(user='username', password='password', proxy_url='')
    my_auth = MicexAuth(my_config)
    if my_auth.is_real_time():
        iss = MicexISSClient(my_config, my_auth, MyDataHandler, MyData)
        iss.get_history_securities('stock',
                                   'shares',
                                   'eqne',
                                   '2010-04-29')
        iss.handler.data.print_history()

if __name__ == '__main__':
    try:
        main()
    except:
        print "Sorry:", sys.exc_type, ":", sys.exc_value
