import pandas as pd
import os

os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")


def get_data(stock, type_needed='NA'):
    get_df = pd.read_hdf('data/HDF5/store.h5', 'RAW',
                         where='Stock == %s' % stock)
    if (type_needed != 'NA'):
        if(type(type_needed) == type('Str')):
            type_needed = [type_needed]
        get_df = get_df[get_df['Message_Type'].isin(type_needed)]
    get_df.dropna(1, how='all', inplace=True)
    return (get_df.reset_index())
