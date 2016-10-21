import pandas as pd
import os
import csv
from tqdm import *
import numpy as np
from heapq import nlargest, nsmallest
from datetime import timedelta
import h5py

# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")


def time_convert(nanosecond_time):
    return (str(timedelta(seconds=nanosecond_time / 1e+9)))


def add_order_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number'],
             'type': message['Buy_Sell_Indicator'],
             'quantity': message['Shares'],
             'price': message['Price']}
    return (order)


def add_MPID_order_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number'],
             'type': message['Buy_Sell_Indicator'],
             'quantity': message['Shares'],
             'price': message['Price']}
    return (order)


def order_excuted_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number'],
             'quantity': message['Executed_Shares']}
    return (order)


def price_order_excuted_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number'],
             'quantity': message['Executed_Shares'],
             'price': message['Execution_Price']}
    return (order)


def canel_order_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number'],
             'quantity': message['Canceled_Shares']}
    return (order)


def delete_order_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Order_Reference_Number']}
    return (order)


def replace_order_handler(message):
    order = {'time': message['Time_Stamp'],
             'id': message['Original_Order_Reference_Number'],
             'newid': message['New_Order_Reference_Number'],
             'quantity': message['Shares'],
             'price': message['Price']}
    return (order)


def full_order_book_output(time, bid, ask, messageType):
    array_value = []
    array_value.append(messageType)
    array_value.append(time)
    for key in sorted(bid):
        array_value.append(key)
        array_value.append(bid[key])
    array_value.append(000)
    for key in sorted(ask):
        array_value.append(key)
        array_value.append(ask[key])
    return (array_value)


def ten_orderbook_output(time, bid, ask, messageType):
    array_value = [0] * 43
    array_value[0] = time
    array_value[1] = ord(messageType)
    bid_position = 2
    ask_position = 23
    for key in reversed(nlargest(10, bid)):
        array_value[bid_position] = key
        array_value[bid_position + 1] = bid[key]
        bid_position += 2
    array_value[22] = 999999999
    for key in nsmallest(10, ask):
        array_value[ask_position] = key
        array_value[ask_position + 1] = ask[key]
        ask_position += 2
    return (array_value)

data_input_dir = os.getcwd() + "/data/order_book/"
data_output_dir = os.getcwd() + "/data/HDF5/hdf5/"

orderbook_message_type = ['A', 'F', 'E', 'C', 'U', 'D', 'X']
print('Start reading raw file...')
stock_input = pd.read_hdf(data_output_dir + 'SPY.h5',
                          'table').replace("", np.nan)
stock_input = stock_input[stock_input[
    'Message_Type'].isin(orderbook_message_type)]
stock_input.dropna(1, how='all', inplace=True)
stock_input = stock_input.loc[
    :, (stock_input != 0).any(axis=0)].reset_index(drop=True)
print('Finished reading raw file...')

bid = {}
ask = {}
order_pool = {'Initial': [0, 0, 0]}
order_book = []
message_record = []
# f = h5py.File("data/order_book/order_book_compressed.hdf5",
#             'w', compression="gzip", compression_opts=9)
f2 = h5py.File("data/order_book/order_book.hdf5", 'w')


for iteration in trange(len(stock_input)):
    this_order = [0.0] * 42
    message = stock_input.iloc[iteration]

    if message.Message_Type == 'A':
        information = add_order_handler(message)
        order_time = information['time']
        order_id = information['id']
        order_type = information['type']
        order_price = information['price']
        order_quantity = information['quantity']

        if order_type == 'B':
            if order_price in list(bid.keys()):
                bid[order_price] += order_quantity
            else:
                bid[order_price] = order_quantity
        else:
            if order_price in list(ask.keys()):
                ask[order_price] += order_quantity
            else:
                ask[order_price] = order_quantity
        order_pool[order_id] = [order_quantity, order_price, order_type]
        message_record.append([order_time, ord('A'), order_price, order_quantity])
        # this_order.append(np.string_(str(order_time) + ":" + str(order_quantity) +
        #       " shares of order has been added at price " + str(order_price)))

    elif message.Message_Type == 'F':
        information = add_MPID_order_handler(message)
        order_time = information['time']
        order_id = information['id']
        order_type = information['type']
        order_price = information['price']
        order_quantity = information['quantity']

        if order_type == 'B':
            if order_price in list(bid.keys()):
                bid[order_price] += order_quantity
            else:
                bid[order_price] = order_quantity
        else:
            if order_price in list(ask.keys()):
                ask[order_price] += order_quantity
            else:
                ask[order_price] = order_quantity
        order_pool[order_id] = [order_quantity, order_price, order_type]
        message_record.append([order_time, ord('F'), order_price, order_quantity])

        # this_order.append(np.string_(str(order_time) + ":" + str(order_quantity) +
        #       " shares of order has been added at price " + str(order_price)))

    elif message.Message_Type == 'E':
        information = order_excuted_handler(message)
        order_time = information['time']
        order_id = information['id']
        executed_quantity = information['quantity']
        order_price = order_pool[order_id][1]
        order_type = order_pool[order_id][2]

        if order_type == 'B':
            bid[order_price] -= executed_quantity
            if bid[order_price] <= 0:
                del bid[order_price]
        else:
            ask[order_price] -= executed_quantity
            if ask[order_price] <= 0:
                del ask[order_price]
        order_pool[order_id][0] -= executed_quantity

        # this_order.append(np.string_(str(order_time) + ": " + str(executed_quantity)
        #       + " shares of order just executed at price "
        #       + str(order_price)))

        if order_pool[order_id][0] <= 0:
            del order_pool[order_id]
        message_record.append([order_time, ord('E'), order_price, executed_quantity])

    elif message.Message_Type == 'C':
        information = price_order_excuted_handler(message)
        order_time = information['time']
        order_id = information['id']
        executed_quantity = information['quantity']
        order_price = order_pool[order_id][1]
        order_type = order_pool[order_id][2]

        if order_type == 'B':
            bid[order_price] -= executed_quantity
            if bid[order_price] <= 0:
                del bid[order_price]
        else:
            ask[order_price] -= executed_quantity
            if ask[order_price] <= 0:
                del ask[order_price]
        order_pool[order_id][0] -= executed_quantity

        # this_order.append(np.string_(str(order_time) + ": " + str(executed_quantity) +
        #       " shares of order just executed at price " + str(information['price'])))

        if order_pool[order_id][0] <= 0:
            del order_pool[order_id]
        message_record.append([order_time, ord('C'), information['price'], executed_quantity])


    elif message.Message_Type == 'U':
        information = replace_order_handler(message)
        order_time = information['time']
        old_order_id = information['id']
        old_quantity = order_pool[old_order_id][0]
        old_price = order_pool[old_order_id][1]
        old_type = order_pool[old_order_id][2]
        new_order_id = information['newid']
        new_price = information['price']
        new_quantity = information['quantity']

        order_pool[new_order_id] = [new_quantity, new_price, old_type]
        del order_pool[old_order_id]

        if old_type == 'B':
            bid[old_price] -= old_quantity
            if bid[old_price] <= 0:
                del bid[old_price]
            if new_price in list(bid.keys()):
                bid[new_price] += new_quantity
            else:
                bid[new_price] = new_quantity
        else:
            ask[old_price] -= old_quantity
            if ask[old_price] <= 0:
                del ask[old_price]
            if new_price in list(ask.keys()):
                ask[new_price] += new_quantity
            else:
                ask[new_price] = new_quantity
        message_record.append([order_time, ord('U'), new_price, new_quantity])


        # this_order.append(np.string_(str(order_time) + ": " + str(old_quantity) +
        #       " shares of order at price " + str(old_price) +
        #       " has been replaced as " + str(new_quantity) + " shares of order at price " +
        #       str(new_price)))

    elif message.Message_Type == 'D':
        information = delete_order_handler(message)
        order_time = information['time']
        order_to_delete = information['id']
        delete_quantity = order_pool[order_to_delete][0]
        order_price = order_pool[order_to_delete][1]
        order_type = order_pool[order_to_delete][2]
        if order_type == 'B':
            bid[order_price] -= delete_quantity
            if bid[order_price] <= 0:
                del bid[order_price]
        else:
            ask[order_price] -= delete_quantity
            if ask[order_price] <= 0:
                del ask[order_price]

        # this_order.append(np.string_(str(order_time) + ":" + str(delete_quantity) +
        #       " shares of order has been deleted at price " + str(order_price)))

        del order_pool[order_to_delete]
        message_record.append([order_time, ord('D'), order_price, delete_quantity])


    elif message.Message_Type == 'X':
        information = canel_order_handler(message)
        order_time = information['time']
        order_to_cancel = information['id']
        cancel_quantity = information['quantity']
        order_price = order_pool[order_to_cancel][1]
        order_type = order_pool[order_to_cancel][2]

        if order_type == 'B':
            bid[order_price] -= cancel_quantity
            if bid[order_price] <= 0:
                del bid[order_price]
        else:
            ask[order_price] -= cancel_quantity
            if ask[order_price] <= 0:
                del ask[order_price]
        order_pool[order_to_cancel][0] -= cancel_quantity
        # this_order.append(np.string_(str(order_time) + ": " + str(cancel_quantity) +
        # " shares of order just canceled  at price " + str(order_price)))

        if order_pool[order_to_cancel][0] <= 0:
            del order_pool[order_to_cancel]
        message_record.append([order_time, ord('X'), order_price, cancel_quantity])


    else:
        this_order.append(np.string_('Wrong Message Type!'))

    # print(full_orderbook_output(information['time'], message.Message_Type, bid, ask))
    temp_list = ten_orderbook_output(
        information['time'], bid, ask, message.Message_Type)
    this_order[0:len(temp_list)] = temp_list
    order_book.append(this_order)

#f['SPY'] = order_book
f2['SPY/orderbook'] = order_book
f2['SPY/record'] = message_record
# f.close()
f2.close()
print("finish writing...")
