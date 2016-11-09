import pandas as pd
import os
import time
import numpy as np
import matplotlib.pyplot as plt
import h5py
from bisect import bisect_left



def time_to_nanosecond(input_time_string):
    ftr = [3600, 60, 1]
    nanosecond_output = sum([a * b for a, b in zip(ftr, [int(i)
                                                         for i in
                                                         input_time_string.split(
                                                             ":")])]) * 1e+9
    return nanosecond_output

# function to extract data in time range needed, and delete all 0 difference data
def data_cleaning(df_input, start_time='10:30:00', end_time='15:00:00',
                  execution=False):
    """
    :param df_input:
    :param start_time:
    :param end_time:
    :return pandas dataframe:
    """
    start_timestamp = time_to_nanosecond(start_time)
    end_timestamp = time_to_nanosecond(end_time)
    if execution:
        df_input = df_input[
            df_input[1].isin([ord('E'), ord('C')])].reset_index(drop=True)
    df_output = df_input[
        start_timestamp <= df_input[0]]
    df_output = df_output[
        df_output[0] <= end_timestamp].reset_index(drop=True)

    execution_time = np.array(df_output[0])
    diff_time = np.diff(execution_time)
    diff_time = np.insert(diff_time, 0, np.nan)
    df_output = df_output[
        diff_time != 0].reset_index(drop=True)
    return df_output


def get_direction(input_val):
    """

    :param input_val:
    :return:
    """
    if input_val == np.nan:
        return np.nan
    elif input_val > 0:
        return 1
    elif input_val == 0:
        return 0
    elif input_val < 0:
        return -1
    else:
        return np.nan


def get_imbalance_rate(df_input, level):
    for i in range(1, (level + 1)):
        try:
            bid_column += df_input['best.bid.q.' + str(i)]
            ask_column += df_input['best.ask.q.' + str(i)]
        except:
            bid_column = df_input['best.bid.q.1'].copy()
            ask_column = df_input['best.ask.q.1'].copy()
    return bid_column / ask_column



#function to find the latest

def takeClosest(myNumber, myList):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0]
    if pos == len(myList):
        return myList[-1]
    if myList[pos] == myNumber:
        return myNumber
    before = myList[pos - 1]
    return before

#_LAG = 1000000

# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
data_input_dir = os.getcwd() + "/data/order_book/"
data_output_dir = os.getcwd() + "/data/Attributes/"
for _LAG in np.linspace(5000, 1000000, 10):
    # read hdf5 file
    print("reading hdf5 file...")
    with h5py.File(data_input_dir + "/order_book.hdf5", 'r') as f:
        order_book = f['SPY/orderbook'][:]
        record = f['SPY/record'][:]

    # transfer np.array to pdans dataframe, for easy cleaning purpose
    orderbook_df = pd.DataFrame(order_book)
    record_df = pd.DataFrame(record)
    orderbook_df[22] = 0.5 * (orderbook_df[20] + orderbook_df[23])


    # plot bid ask spread
    # bid_ask_spread = orderbook_df[23] - orderbook_df[20]
    # bid_ask_spread = bid_ask_spread[abs(bid_ask_spread)<0.1]
    # plt.hist(bid_ask_spread, bins=200)
    # plt.show()


    # step 1: get the data and transfer it to data frame, from 10:30:00 to 15:30:00
    time_to_start = '10:30:00'
    time_to_end = '15:00:00'
    execution_orderbook_df = data_cleaning(orderbook_df, time_to_start,
                                           time_to_end, True)
    execution_record_df = data_cleaning(record_df, time_to_start,
                                        time_to_end, True)

    orderbook_df.set_index(0, inplace=True)
    execution_orderbook_df.set_index(0, inplace=True)
    execution_record_df.set_index(0, inplace=True)


    # drop column 1 which stands for order type
    execution_orderbook_df.drop(1, 1, inplace=True)
    execution_record_df.drop(1, 1, inplace=True)
    # Start build training attributes
    execution_orderbook_df = execution_orderbook_df[[22]
                                                    + list(range(2, 22))
                                                    + list(
        range(23, execution_orderbook_df.columns[-1] + 1))]
    execution_orderbook_pctchange_df = execution_orderbook_df.pct_change(1)
    execution_record_pctchange_df = execution_record_df.pct_change(1)

    # caculate the mid price between best bid and best ask
    Y = pd.DataFrame(index=execution_record_df.index,
                     columns=['future.mid.price', 'direction.by.midp', 'direction.by.e'])

    timestamp_needed = []


    for i in np.array(execution_record_df.index)+_LAG:
        timestamp_needed.append(takeClosest(i, orderbook_df.index))
    print('found all future timestamp!')
    search_df = orderbook_df.groupby(orderbook_df.index).last()
    Y['future.mid.price'] = search_df.loc[list(timestamp_needed)][22].tolist()


    execution_record_df.columns = ['exe.p', 'exe.v']
    execution_record_pctchange_df.columns = ['exe.p.d', 'exe.v.c']



    order_book_column = []
    order_book_column.append("mid.price")
    for i in reversed(range(1, 11)):
        order_book_column.append('best.bid.p.' + str(i))
        order_book_column.append('best.bid.q.' + str(i))
    for i in range(1, 11):
        order_book_column.append('best.ask.p.' + str(i))
        order_book_column.append('best.ask.q.' + str(i))
    execution_orderbook_df.columns = order_book_column

    order_book_pctc_column = []
    order_book_pctc_column.append("mid.price.c")
    for i in reversed(range(1, 11)):
        order_book_pctc_column.append('best.bid.p.c' + str(i))
        order_book_pctc_column.append('best.bid.q.c' + str(i))
    for i in range(1, 11):
        order_book_pctc_column.append('best.ask.p.c' + str(i))
        order_book_pctc_column.append('best.ask.q.c' + str(i))
    execution_orderbook_pctchange_df.columns = order_book_pctc_column

    # caluclate the imbalance rate
    print("calulating  imbalance rate...")
    imbalance_df = pd.DataFrame(index=execution_orderbook_df.index)
    for i in range(1, 11):
        imbalance_df['imbalance.' +
    str(i)] = np.array(
    get_imbalance_rate(execution_orderbook_df, i))

    # merge output
    output_df = Y.join(execution_record_df)
    output_df = output_df.join(execution_record_pctchange_df)
    output_df = output_df.join(execution_orderbook_df)
    output_df = output_df.join(execution_orderbook_pctchange_df)
    output_df = output_df.join(imbalance_df)

    output_col = list(output_df.columns)
    output_col.remove('mid.price')
    output_col.remove('exe.p')
    output_df = output_df[['mid.price']+['exe.p'] + output_col]

    vgetdirection = np.vectorize(get_direction)
    output_df['direction.by.midp'] = vgetdirection(output_df['future.mid.price'] - output_df['mid.price'])
    output_df['direction.by.e'] = vgetdirection(output_df['future.mid.price'] - output_df['exe.p'])
    output_name = data_output_dir + "Attributes" + time.strftime("_%m_%d") + '_lag' + str(_LAG) + ".csv"
    output_df.to_csv(output_name)


    output_matrix = output_df.as_matrix()
    #with h5py.File(data_input_dir + "/order_book.hdf5", 'r+') as f:
    #   f["SPY/Attributes" + time.strftime("_%m_%d")] = output_matrix

    print("finished task!")






# diff_time_sub = diff_time[diff_time < 10000]
# plt.hist(diff_time_sub, bins=20)
# plt.show()
# frequency = []
# for i in sorted(set(diff_time_sub)):
#     frequency.append(sum(diff_time_sub == i))
# for k, v in zip(sorted(set(diff_time_sub)), frequency):
#     print(k, v)
#
# hist, bins = np.histogram(diff_time, bins=200)
# width = 0.7 * (bins[1] - bins[0])
# center = (bins[:-1] + bins[1:]) / 2
# plt.bar(center, hist, align='center', width=width)
#
# plt.show()
#
#
# plt.clf()


# lags = [500, 2000, 5000, 10000, 30000, 50000, 100000]
# for _LAG in lags:
#     timestamp_needed = []
#     for i in np.array(execution_record_df.index) + _LAG:
#         timestamp_needed.append(takeClosest(i, orderbook_df.index))
#     print(np.sum(timestamp_needed != execution_record_df.index))
