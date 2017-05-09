"""
    Author: Jon Ander Gomez Adrian (jon@dsic.upv.es, http://www.dsic.upv.es/~jon)
    Version: 1.0
    Date: November 2015
    Universitat Politecnica de Valencia
    Technical University of Valencia TU.VLC

    Class KMeans for being used in the distributed implementation of the K-Means algorithm over Spark
"""

import numpy

try:
    import cPickle as pickle
except:
    import pickle


# ---------------------------------------------------------------------------------            
class KMeans:

# ---------------------------------------------------------------------------------            
    def __init__(self, nc=2, dim=1 ):
        self.num_clusters = nc
        self.dim = dim
        self.means = numpy.zeros( [ self.num_clusters, dim ] )
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def initialize_random_from( self, samples ):
        for c in range(self.num_clusters):
            n = numpy.random.randint( len(samples) )
            self.means[c,:] = samples[n,:]
        return self
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def show_means( self ):
        for c in range(self.num_clusters):
            print( self.means[c] )
# ---------------------------------------------------------------------------------            

    def __len__(self): return self.num_clusters

# ---------------------------------------------------------------------------------            
    def assign_sample_to_a_cluster( self, sample ):
        c=0
        diff = sample - self.means[0]
        min_dist = numpy.dot(diff, diff)
        for k in range(self.num_clusters):
            diff = sample - self.means[k]
            dist = numpy.dot(diff, diff)
            if dist < min_dist:
                c = k
                min_dist = dist
            #
        #
        min_dist = numpy.sqrt(min_dist)
        return (c, [min_dist, sample, 1] )
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def save_to_pickle( self, filename ):
        with open( filename, 'w' ) as f: pickle.dump( self, f )
        f.close()
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def load_from_pickle( filename ):
        with open( filename, "r" ) as f:
            print( type(f) )
            _kmeans = pickle.load( f )
        f.close()
        return _kmeans
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def save_to_text( self, filename ):
        with open( filename, 'w' ) as f:
            f.write( 'num_clusters %d\n' % self.num_clusters )
            f.write( 'dim %d\n' % self.dim )
            for k in range(self.num_clusters):
                f.write( 'cluster %d\n' % k )
                f.write( 'mean ' )
                f.write( ' '.join( ' {:e} '.format(value) for value in self.means[k] ) )
                f.write( '\n' )
        f.close()
# ---------------------------------------------------------------------------------            

# ---------------------------------------------------------------------------------            
    def load_from_text( filename ):
        with open( filename, 'r' ) as f:
            nc=0
            dim=0
            c=0
            _means = []
            for line in f:
                parts = line.split()
                if 'num_clusters' == parts[0] :
                    nc = int(parts[1])
                elif 'dim' == parts[0] :
                    dim = int(parts[1])
                elif 'cluster' == parts[0] :
                    if c != int(parts[1]):
                        raise Exception( 'cluster indexes not saved sequentially!' )
                    c=c+1
                elif 'mean' == parts[0] :
                    _means.append( [float(value) for value in parts[1:] ] )
                else:                        
                    raise Exception( 'unexpected line in file!' )
            #
            _kmeans = KMeans( nc=nc, dim=dim )
            _kmeans.means = numpy.array( _means )
        f.close()            
        return _kmeans
# ---------------------------------------------------------------------------------            
