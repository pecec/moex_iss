import os.path, time
import numpy as np
import matplotlib.style
import matplotlib as mpl
mpl.style.use('classic')
import matplotlib.pyplot as plt

fname = 'RIH8 070318 19-05-50+0300.txt'

def readZeroTime( fname ):
    if not os.path.isfile( fname ):
        raise ValueError( 'wrong file name: %s' % fname )
    f = open( fname )
    line = f.readline()
    f.close()
    return int( line.split( ' ' )[-1] )
    

zeroTime = readZeroTime( fname )

if not os.path.isfile( fname ):
    raise ValueError( 'wrong file name: %s' % fname )


trades = np.loadtxt( fname, skiprows = 1 )

plt.clf()

plt.plot( trades[ :, 0 ], trades[ :, 1 ], 'o-', ms = 2, color = 'b', mec = 'b' )

plt.xlabel( 'Time, s' )
plt.ylabel( fname[ : fname.find( ' ' ) ] )

plt.annotate( 'start: %s' % ( time.strftime( '%Y-%m-%d %H:%M:%S%z', time.localtime( zeroTime + trades[ 0, 0 ] ) ) ),
            xy=(0.1,0.95), xycoords='axes fraction',
            fontsize=11, horizontalalignment='left', verticalalignment='top' )

plt.annotate( 'end: %s' % ( time.strftime( '%Y-%m-%d %H:%M:%S%z', time.localtime( zeroTime + trades[ -1, 0 ] ) ) ),
            xy=(0.1,0.90), xycoords='axes fraction',
            fontsize=11, horizontalalignment='left', verticalalignment='top' )

plt.savefig( fname[ : fname.find( '.' ) ] + '.png')

plt.show()
    
