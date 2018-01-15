import pandas as pd
import numpy as np
import glob


def csv_to_np_array(folder = '/home/thijs-gerrit/Documents/DataFrame',
                    batch_size=100000,
                    array_file = 'dataframe.array'):
    
    # list of files that need to be read
    files = glob.glob(folder + '/*.csv')

    print('reading..')
    # read the files one by one
    df = pd.concat((pd.read_csv(f, header=1, names=['id', 'lon', 'lat', 'year', 'month', 'day', 'hour', 
                                                    'minute', 'speed', 'fg', 'dr', 'rh','vvn', 'vvx']) for f in files))
    
    print('sorting..')
    # sort on date. From old to new
    df.sort_values(['year', 'month', 'day', 'hour', 'minute'], inplace=True)

    # determine how many batches are needed to load everything into the numpy array with the given batch_size
    length = df.shape[0]
    no_of_batches = np.ceil(length / batch_size)

    # initiliaze numpy memmap
    np_arr = np.memmap(array_file, dtype='float64', mode='w+', shape=(length,6), order='C')

    # initialize values
    lower_index = 0
    upper_index = batch_size + 1

    print('start writing to array')
    # write the batches into the numpy array
    for _ in range(int(no_of_batches)):
        print('batch {0}'.format(_))
        array = df.iloc[lower_index:upper_index, 8:].values
        np_arr[lower_index:upper_index,:] = array[:]

        lower_index += batch_size
        upper_index += batch_size

        if lower_index >= length:
            lower_index = length-1
        if upper_index >= length:
            upper_index = length-1
    
    return np_arr
