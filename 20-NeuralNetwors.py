# ------------------------------------------------------------------- #
#                                                                     #
#	MASTER EN BIG DATA ANALYTICS                                      #
#                                                                     #
#	TRABAJO FINAL DE MASTER                                           #
#                                                                     #
#                                                                     #
#	- CARLOS MARTINEZ GOMEZ                                           #
#	- ENRIQUE CASTELLO FERRE                                          #
#                                                                     #
# ------------------------------------------------------------------- #

# ....................................................................
# 
# ....................................................................


# Load project utils script
from tfm_utils import *

# --------------------------------------------------------------------------------------------------------------------
import numpy
import pandas
# --------------------------------------------------------------------------------------------------------------------
from matplotlib import pyplot
from sklearn.preprocessing import StandardScaler
# --------------------------------------------------------------------------------------------------------------------
import keras
from keras.models import Sequential, Model, load_model
from keras.layers import Dense, Activation, Dropout, BatchNormalization, GaussianNoise
from keras.layers import Input, LSTM, Reshape
from keras.layers import PReLU
from keras.layers.core import Flatten
from keras.layers.convolutional import Conv2D
from keras.constraints import maxnorm
from keras.optimizers import RMSprop, SGD
# --------------------------------------------------------------------------------------------------------------------

def mean_absolute_error( y_true, y_pred ):
    return abs(y_true - y_pred).mean(axis=-1)

def mean_absolute_percentage_error( y_true, y_pred, epsilon=0.1 ):
    diff = abs(y_true - y_pred) / numpy.maximum( abs(y_true), epsilon )
    return 100. * diff.mean(axis=-1)

def mean_squared_error( y_true, y_pred ):
    return ((y_true - y_pred)**2).mean(axis=-1)
    

def str_to_date( s ):
    # Assuming YYYY-MM-DD
    if type(s) == str:
        return datetime.date( int(s[0:4]), int(s[5:7]), int(s[8:10] ) )
    elif type(s) == list or type(s) == numpy.ndarray:
        l=list()
        for x in s:
            l.append( str_to_date(x) )
        if type(s) == list:
            return l
        else:
            return numpy.array(l)
    else:
        raise Exception( 'Incompatible data type of input! ', type(s) )


def prepare_ann_1( X, W, M, Y, n1=1024, n2=512, n3=256 ):

    #
    power_input = Input( shape=( X.shape[1], X.shape[2], 1), name='power_input' )
    week_input  = Input( shape=( W.shape[1], ), name='week_input' )
    month_input = Input( shape=( M.shape[1], ), name='month_input' )
    
	#
    layer1 = Conv2D( filters=7, kernel_size=[3,3], padding='valid', )(power_input)
    #layer2 = Conv2D( filters=7, kernel_size=[5,5], padding='valid', )(power_input)
    #layer3 = Conv2D( filters=7, kernel_size=[7,7], padding='valid', )(power_input)
    layer1 = Flatten()(layer1)
    #layer2 = Flatten()(layer2)
    #layer3 = Flatten()(layer3)
    #layer = keras.layers.concatenate( [layer1, layer2, layer3] )
    layer = layer1
    #
    #layer = GaussianNoise( 0.1 )(power_input)
    #layer = power_input
    #
    #layer = BatchNormalization()(layer)
    layer = Dense( n1, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    layer = BatchNormalization()(layer)
    layer = Dense( n2, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = keras.layers.concatenate( [layer, week_input, month_input] )
    #
    layer = Dense( n3, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = BatchNormalization()(layer)
    layer = Dense( n3, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = BatchNormalization()(layer)
    power_output = Dense( Y.shape[1], activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    #
    model = Model( inputs=[power_input,week_input,month_input], outputs=[power_output] )
    #
    #optimizer = SGD( lr=0.01 )
    optimizer = RMSprop()
    #optimizer = RMSprop( lr=0.01 )
    #loss=[keras.losses.mean_squared_error,keras.losses.mean_absolute_error,keras.losses.mean_absolute_percentage_error]
    loss=keras.losses.mean_squared_error
    #loss=keras.losses.categorical_crossentropy
    #metrics=['mse','mae','mape']
    metrics=['mape']
    #metrics=None
    model.compile( loss=loss, metrics=metrics, optimizer=optimizer )
    #
    return model

def prepare_ann_2( X, W, M, Y , n1=512, n2=1024):
    #
    power_input = Input( shape=( X.shape[1], ), name='power_input' )
    week_input  = Input( shape=( W.shape[1], ), name='week_input' )
    month_input = Input( shape=( M.shape[1], ), name='month_input' )
    #
    layer = GaussianNoise( 0.1 )(power_input)
    #layer = power_input
    #
    layer = BatchNormalization()(layer)
    layer = Dense( n1, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    layer = BatchNormalization()(layer)
    layer = Dense( n1, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = keras.layers.concatenate( [layer, week_input, month_input] )
    #
    layer = Dense( n2, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = BatchNormalization()(layer)
    layer = Dense( n2, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    layer = BatchNormalization()(layer)
    power_output = Dense( Y.shape[1], activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    #
    model = Model( inputs=[power_input,week_input,month_input], outputs=[power_output] )
    #
    #optimizer = SGD( lr=0.01 )
    optimizer = RMSprop()
    #optimizer = RMSprop( lr=0.01 )
    #loss=[keras.losses.mean_squared_error,keras.losses.mean_absolute_error,keras.losses.mean_absolute_percentage_error]
    loss=keras.losses.mean_squared_error
    #loss=keras.losses.categorical_crossentropy
    #metrics=['mse','mae','mape']
    metrics=['mape']
    #metrics=None
    model.compile( loss=loss, metrics=metrics, optimizer=optimizer )
    #
    return model

def prepare_ann_3( X, W, M, Y, n1=512, n2=256 ):
    #
    power_input = Input( shape=( X.shape[1], X.shape[2] ), name='power_input' )
    week_input  = Input( shape=( W.shape[1], ), name='week_input' )
    month_input = Input( shape=( M.shape[1], ), name='month_input' )
    #
    #layer = GaussianNoise( 0.1 )(power_input)
    layer = power_input
    #
    #layer = LSTM( 7, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    layer = LSTM( 17, activation='tanh', kernel_constraint=maxnorm(3.0) )(layer)
    #
    layer = keras.layers.concatenate( [layer, week_input, month_input] )
    #
    layer = Dense( n1, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    #layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    #layer = BatchNormalization()(layer)
    layer = Dense( n2, activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    #layer = Dropout(0.5)(layer)
    layer = Activation( 'relu' )(layer)
    #
    #layer = BatchNormalization()(layer)
    power_output = Dense( Y.shape[1], activation='linear', kernel_constraint=maxnorm(3.0) )(layer)
    #
    model = Model( inputs=[power_input,week_input,month_input], outputs=[power_output] )
    #
    #optimizer = SGD( lr=0.01 )
    optimizer = RMSprop()
    #optimizer = RMSprop( lr=0.01 )
    #loss=[keras.losses.mean_squared_error,keras.losses.mean_absolute_error,keras.losses.mean_absolute_percentage_error]
    loss=keras.losses.mean_squared_error
    #loss=keras.losses.categorical_crossentropy
    #metrics=['mse','mae','mape']
    metrics=['mape']
    #metrics=None
    model.compile( loss=loss, metrics=metrics, optimizer=optimizer )
    #
    return model

def show_errors( time, Y_true, Y_predict, with_graphs=False ):

    mae  = mean_absolute_error(            Y_true, Y_predict )
    mape = mean_absolute_percentage_error( Y_true, Y_predict )
    mse  = mean_squared_error(             Y_true, Y_predict )

    print( 'MSE   %f ' % mse.mean() )
    print( 'MAE   %f ' % mae.mean() )
    print( 'MAPE  %7.3f%% ' % mape.mean() )

    if with_graphs:
        pyplot.plot( time, Y_predict.mean(axis=1), color='blue',  lw=7, alpha=0.2 )
        pyplot.plot( time, Y_true.mean(axis=1),    color='blue',  lw=2 )
        pyplot.grid()
        pyplot.show()

        pyplot.plot( time, mape, color='red',   lw=1 )
        pyplot.grid()
        pyplot.show()


def tupleToStr(t):
	s = ""
	for i in xrange(len(t)):
		s += "-" + str(t[i])
	s = s[1:]
	return s
		

verboseFit = 0
maxPreviousDays = 15
#maxEpochs = 1500
#epochsIncrement = 100
maxEpochs = 1000
epochsIncrement = 20
nnTypes = ["Feed Forward", "Convolutional", "LSTM"]
model_datasets = ["/home/kike/Escritorio/TFM/Data/NeuralNetworksDatasets/0154/d/075/0004"]
resultsFilename = "/home/kike/Escritorio/TFM/Results/NN-Results.csv"

resultsFile = open(resultsFilename, "w")
header = ["Model", "Component", "NN-Type", "PreviousDays", "Topology", "Epochs", "MSE1", "MAE1", "MAPE1", "MSE2", "MAE2", "MAPE2"]
resultsFile.write(toCSVLine(header) + "\n")

for model_dataset in model_datasets:

	model = model_dataset.split("NeuralNetworksDatasets")
	model = model[1]

	# Get current time to monitorize execution time
	modelExecutionStartTime = time.time()

	if verbose:
		print getCurrentDateTimeString() + " - Generating NNs for " + model_dataset
	
	model_components = int(os.path.basename(model_dataset))
	
	for component in xrange(1, model_components + 1):
		
		if verbose:
			print getCurrentDateTimeString() + " - Generating NN for " + str(component) + " component"


		dataset_filename = model_dataset + "/" + str(component) + "/dataset.csv"

		# Inicializamos las variables con los datos
		D = list()
		P = list()
		W = list()
		M = list()

		# Abrimos el dataset
		f = open(dataset_filename, "rt")

		# Leemos el dataset
		for line in f:
			parts = line.split(csvDelimiter)
			D.append(datetime.date(int(parts[0]), int(parts[1]), int(parts[2])))
			M.append(int(parts[1]))
			W.append(int(parts[3]))
			P.append(numpy.array([float(x) for x in parts[4:]]))

		f.close()

		P = numpy.array(P)
		W = numpy.array(W)
		M = numpy.array(M)
		D = numpy.array(D)

		power_scaler = StandardScaler()
		power_scaler.fit(P)
		scaled_power = power_scaler.transform(P)

		for t in range(1,len(D)):
			if D[t]-D[t-1] != datetime.timedelta(days=1) :
				raise Exception( "%s and %s are not consecutive days" % (str(D[t-1]), str(D[t])) )


		_w_ = numpy.zeros( [ len(W), W.max()-W.min()+1 ] )
		_w_[ numpy.arange(len(W)), W-W.min() ] = 1
		_m_ = numpy.zeros( [ len(M), M.max()-M.min()+1 ] )
		_m_[ numpy.arange(len(M)), M-M.min() ] = 1

		for nnType in nnTypes:
			if verbose:
				print getCurrentDateTimeString() + " - Generating " + nnType + " NN" 

			
			for previous_days in xrange(1, maxPreviousDays + 1):
				if verbose:
					print getCurrentDateTimeString() + " - With " + str(previous_days) + " previous days"

				topologies = []
				if nnType == "Feed Forward":
					X = numpy.zeros( [ len(scaled_power) - previous_days - 1, previous_days * scaled_power.shape[1] ] )
					topologies = [(512, 1024), (1024, 2048), (512, 512), (256, 512), (1024, 512)]

				elif nnType == "Convolutional":
					X = numpy.zeros( [ len(scaled_power) - previous_days - 1, previous_days, scaled_power.shape[1], 1 ] )
					topologies = [(1024, 512, 256), (2048, 1024, 512), (512, 512, 512), (1024, 1024, 1024), (512, 258, 128), (256, 512, 1024)]

				elif nnType == "LSTM":
					X = numpy.zeros( [ len(scaled_power) - previous_days - 1, previous_days, scaled_power.shape[1] ] )
					topologies = [(512, 256), (1024, 512), (512, 512), (256, 128), (256, 512)]

				else:
					X = numpy.zeros( [ len(scaled_power) - previous_days - 1, previous_days * scaled_power.shape[1] ] )

				Y = numpy.zeros( [ len(X), scaled_power.shape[1] ] )


				for t in range(len(X)):

					if nnType == "Feed Forward":
						X[t,:] = scaled_power[t:t+previous_days  ,:].ravel()		

					elif nnType == "Convolutional":
						X[t,:,:,0] = scaled_power[t:t+previous_days  ,:]

					elif nnType == "LSTM":
						X[t,:,:] = scaled_power[t:t+previous_days  ,:]	

					else:
						X[t,:] = scaled_power[t:t+previous_days  ,:].ravel()

				Y[:,:] = scaled_power[  previous_days+1:,:]
				y_true = P[  previous_days+1:,:]
				W = _w_[previous_days+1:]
				M = _m_[previous_days+1:]

				n = int(0.8 * len(X))

				for topology in topologies:
					if verbose:
						print topology
				
					if nnType == "Feed Forward":
						ann = prepare_ann_2( X, W, M, Y, topology[0], topology[1] )
						#ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=100, epochs=100, verbose=verboseFit, shuffle=True )
						#y_predict = ann.predict( [X[n:], W[n:], M[n:]], batch_size=1, verbose=verboseFit )
					

					elif nnType == "Convolutional":
						ann = prepare_ann_1( X, W, M, Y, topology[0], topology[1], topology[2] )
						#ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=100, epochs=100, verbose=verboseFit, shuffle=True )
						#y_predict = ann.predict( [X[n:], W[n:], M[n:]], batch_size=1, verbose=verboseFit )

					
					elif nnType == "LSTM":
						ann = prepare_ann_3( X, W, M, Y, topology[0], topology[1] )
						#ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=1, epochs=10, verbose=verboseFit, shuffle=False )
						#y_predict = ann.predict( [X[:n],W[:n],M[:n]], batch_size=1, verbose=verboseFit )
					
					else:
						ann = None

					#if verbose:
					#	ann.summary()


					epochs = epochsIncrement
					while epochs <= maxEpochs:
						if verbose:
							print getCurrentDateTimeString() + " - Epochs " + str(epochs)
	
						if nnType == "Feed Forward":
							ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=100, epochs=epochs, verbose=verboseFit, shuffle=True )
							y_predict = ann.predict( [X[n:], W[n:], M[n:]], batch_size=1, verbose=verboseFit )

						elif nnType == "Convolutional":
							ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=100, epochs=epochs, verbose=verboseFit, shuffle=True )
							y_predict = ann.predict( [X[n:], W[n:], M[n:]], batch_size=1, verbose=verboseFit )
					
						elif nnType == "LSTM":
							ann.fit( [X[:n],W[:n],M[:n]], Y[:n], batch_size=1, epochs=epocs/10, verbose=verboseFit, shuffle=False )
							y_predict = ann.predict( [X[:n],W[:n],M[:n]], batch_size=1, verbose=verboseFit )

						if power_scaler is not None:
							y_predict = power_scaler.inverse_transform(y_predict)
				
						#show_errors( D[n+previous_days:-3], y_true[n:-2], y_predict[0:-2], with_graphs=False )
						#show_errors( D[n+previous_days:-3], y_true[n:-2], y_predict[2:  ], with_graphs=False )
					
				
						mae  = mean_absolute_error(y_true[n:-2], y_predict[0:-2])
						mape = mean_absolute_percentage_error(y_true[n:-2], y_predict[0:-2])
						mse  = mean_squared_error(y_true[n:-2], y_predict[0:-2])
					
						#print( 'MSE   %f ' % mse.mean() )
						#print( 'MAE   %f ' % mae.mean() )
						#print( 'MAPE  %7.3f%% ' % mape.mean() )
						strMse = '%f ' % mse.mean()
						strMae = '%f ' % mae.mean()
						strMape = '%7.3f%% ' % mape.mean()
					
						mae2  = mean_absolute_error(y_true[n:-2], y_predict[2:  ])
						mape2 = mean_absolute_percentage_error(y_true[n:-2], y_predict[2:  ])
						mse2  = mean_squared_error(y_true[n:-2], y_predict[2:  ])

						#print( 'MSE   %f ' % mse.mean() )
						#print( 'MAE   %f ' % mae.mean() )
						#print( 'MAPE  %7.3f%% ' % mape.mean() )
						strMse2 = '%f ' % mse2.mean()
						strMae2 = '%f ' % mae2.mean()
						strMape2 = '%7.3f%% ' % mape2.mean()

						
						resultLine = [model, component, nnType.replace(" ", ""), previous_days, tupleToStr(topology), epochs, strMse, strMae, strMape, strMse2, strMae2, strMape2]
						resultsFile.write(toCSVLine(resultLine) + "\n")

						epochs += epochsIncrement
						


	modelExecutionEndTime = time.time()
	if verbose:
		print getExecutionTimeMsg(modelExecutionStartTime, modelExecutionEndTime)


