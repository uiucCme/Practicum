import struct
import os
import pandas as pd
import numpy as np
import multiprocessing
from multiprocessing import Process
import threading
from tqdm import *
from datetime import timedelta

# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
def clean_name(input_name):
    input_name = input_name.replace('.', "")
    input_name = input_name.replace("-", "minus")
    input_name = input_name.replace('+', 'plus').replace('*', "star")
    input_name = input_name.replace('=', 'equal').replace('#', "pound")
    return(input_name)


def write_hdf5(DataFrameDict):
    lock.acquire()
    store = pd.HDFStore('data/HDF5/store.h5', "a", complevel=9, complib='zlib')
    for key in tqdm(DataFrameDict.keys()):
        temp_df = pd.DataFrame(DataFrameDict[key], columns=columns).replace("", np.nan, inplace=True)
        try:
            store[key] = store[key].append(temp_df)
        except:
            store[key] = pd.DataFrame(temp_df)
    #print(str(timedelta(seconds=store['AAPL']['Time Stamp'][0] / 1e+9)))
    store.close()
    del temp_dict
    lock.release()



def find_index(a):
    result = []
    for x in a:
        result.append(columns.index(x))
    return (result)


def find_name(code):
    return (code_map.loc[code_map['Stock Locate'] == code].iloc[0, 1])


def form_list(list):
    for position in range(0, len(list)):
        if type(list[position]) == type(b'byte'):
            list[position] = list[position].decode("utf-8")
            list[position] = list[position].strip()
    return (list)


def complete_result(list):
    missing = 10 - len(list)
    while (missing > 0):
        list += ['NA']
        missing -= 1
    return (list)


def System_Event_Message(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[-1] = chr(ord(array[-1]))
    array = form_list(array)
    complete_array = [''] * 56
    array_index = System_Event_Message_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Stock_Directory(message):
    array = [chr(message[0])] + \
                 list(struct.unpack("!HH6s8sccIcc2scccccIc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Stock_Directory_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Stock_Trading(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8scc4s", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Stock_Trading_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Reg_SHO(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Reg_SHO_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Market_Participant_Position(message):
    array = [chr(message[0])] + \
                 list(struct.unpack("!HH6s4s8sccc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Market_Participant_Position_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def MWCB_Decline(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQQQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = MWCB_Decline_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def MWCB_Status(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = MWCB_Status_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def IPOUpdate(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8cIcI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = IPOUpdate_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Add_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQcI8sI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Add_Order_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Add_MPID_Order(message):
    array = [chr(message[0])] + \
                 list(struct.unpack("!HH6sQcI8sI4s", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Add_MPID_Order_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Excueted_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQIQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Excueted_Order_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Excueted_Price_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQIQcI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Excueted_Price_Order_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Order_Cancel(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Order_Cancel_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Order_Delete(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Order_Delete_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Order_Replace(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQQII", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[7] = array[7] / 10000
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Order_Replace_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQcIQIQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Trade_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Cross_Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ8sIQc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Cross_Trade_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def Broken_Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = Broken_Trade_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def NOII(message):
    array = [chr(message[0])] + \
                 list(struct.unpack("!HH6sQQc8sIIIcc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = NOII_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def RPII(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array = form_list(array)
    complete_array = [''] * 56
    array_index = RPII_index
    p = 0
    for i in array_index:
        complete_array[i] = array[p]
        p += 1
    return (complete_array)


def ParseMessage(message, messageType):
    if messageType == 'S':
        return (System_Event_Message(message))
    elif messageType == 'R':
        return (Stock_Directory(message))
    elif messageType == 'H':
        return (Stock_Trading(message))
    elif messageType == 'Y':
        return (Reg_SHO(message))
    elif messageType == 'L':
        return (Market_Participant_Position(message))
    elif messageType == 'V':
        return (MWCB_Decline(message))
    elif messageType == 'W':
        return (MWCB_Status(message))
    elif messageType == 'K':
        return (IPOUpdate(message))
    elif messageType == 'A':
        return (Add_Order(message))
    elif messageType == 'F':
        return (Add_MPID_Order(message))
    elif messageType == 'E':
        return (Excueted_Order(message))
    elif messageType == 'C':
        return (Excueted_Price_Order(message))
    elif messageType == 'X':
        return (Order_Cancel(message))
    elif messageType == 'D':
        return (Order_Delete(message))
    elif messageType == 'U':
        return (Order_Replace(message))
    elif messageType == 'P':
        return (Trade(message))
    elif messageType == 'Q':
        return (Cross_Trade(message))
    elif messageType == 'B':
        return (Broken_Trade(message))
    elif messageType == 'I':
        return (NOII(message))
    elif messageType == 'N':
        return (RPII(message))
    else:
        print('message type not found!')
        return


columns = ['Attribution', 'Authenticity', 'Bereached Level',
           'Buy/Sell Indicator', 'Canceled Shares', 'Cross Price',
           'Cross Type', 'Current Reference Price', 'ETP Flag',
           'ETP Leverage Factor', 'Event Code', 'Executed Shares',
           'Execution Price', 'Far price', 'Financial Status Indicator',
           'IPO Flag', 'IPO Price', 'IPO Quotation Release Qualifier',
           'IPO Quotation Release Time', 'Imbalance Direction', 'Imbalance Shares',
           'Interest Flag', 'Inverse Indicator', 'Issue Classification',
           'Issue Subtype', 'LUID Reference Price Tier', 'Level 1',
           'Level 2', 'Level 3', 'MPID',
           'Market Category', 'Market Maker Mode', 'Market Participant State',
           'Match Number', 'Message Type', 'Near Price',
           'New Order Reference Number', 'Order Reference Number', 'Original Order Reference Number',
           'Paired Shares', 'Price', 'Price Variation Indicator',
           'Primary Market Maker', 'Printable', 'Reason',
           'Reg SHO Action', 'Reserved', 'Round Lot Size',
           'Round Lots Only', 'Shares', 'Short Sale Threshold Indicator',
           'Stock', 'Stock Locate', 'Time Stamp',
           'Tracking Number', 'Trading State']

System_Event_Message_index = find_index(['Message Type',
                                         'Stock Locate',
                                         'Tracking Number',
                                         'Time Stamp',
                                         'Event Code'])

Stock_Directory_index = find_index(['Message Type',
                                    'Stock Locate',
                                    'Tracking Number',
                                    'Time Stamp',
                                    'Stock',
                                    'Market Category',
                                    'Financial Status Indicator',
                                    'Round Lot Size',
                                    'Round Lots Only',
                                    'Issue Classification',
                                    'Issue Subtype',
                                    'Authenticity',
                                    'Short Sale Threshold Indicator',
                                    'IPO Flag',
                                    'LUID Reference Price Tier',
                                    'ETP Flag',
                                    'ETP Leverage Factor',
                                    'Inverse Indicator'])

Stock_Trading_index = find_index(['Message Type',
                                  'Stock Locate',
                                  'Tracking Number',
                                  'Time Stamp',
                                  'Stock',
                                  'Trading State',
                                  'Reserved',
                                  'Reason'])

Reg_SHO_index = find_index(['Message Type',
                            'Stock Locate',
                            'Tracking Number',
                            'Time Stamp',
                            'Stock',
                            'Reg SHO Action'])

Market_Participant_Position_index = find_index(['Message Type',
                                                'Stock Locate',
                                                'Tracking Number',
                                                'Time Stamp',
                                                'MPID',
                                                'Stock',
                                                'Primary Market Maker',
                                                'Market Maker Mode',
                                                'Market Participant State'])

MWCB_Decline_index = find_index(['Message Type',
                                 'Stock Locate',
                                 'Tracking Number',
                                 'Time Stamp',
                                 'Level 1',
                                 'Level 2',
                                 'Level 3'])

MWCB_Status_index = find_index(['Message Type',
                                'Stock Locate',
                                'Tracking Number',
                                'Time Stamp',
                                'Bereached Level'])

IPOUpdate_index = find_index(['Message Type',
                              'Stock Locate',
                              'Tracking Number',
                              'Time Stamp',
                              'Stock',
                              'IPO Quotation Release Time',
                              'IPO Quotation Release Qualifier',
                              'IPO Price'])

Add_Order_index = find_index(['Message Type',
                              'Stock Locate',
                              'Tracking Number',
                              'Time Stamp',
                              'Order Reference Number',
                              'Buy/Sell Indicator',
                              'Shares',
                              'Stock',
                              'Price'])

Add_MPID_Order_index = find_index(['Message Type',
                                   'Stock Locate',
                                   'Tracking Number',
                                   'Time Stamp',
                                   'Order Reference Number',
                                   'Buy/Sell Indicator',
                                   'Shares',
                                   'Stock',
                                   'Price',
                                   'Attribution'])

Excueted_Order_index = find_index(['Message Type',
                                   'Stock Locate',
                                   'Tracking Number',
                                   'Time Stamp',
                                   'Order Reference Number',
                                   'Executed Shares',
                                   'Match Number'])

Excueted_Price_Order_index = find_index(['Message Type',
                                         'Stock Locate',
                                         'Tracking Number',
                                         'Time Stamp',
                                         'Order Reference Number',
                                         'Executed Shares',
                                         'Match Number',
                                         'Printable',
                                         'Execution Price'])

Order_Cancel_index = find_index(['Message Type',
                                 'Stock Locate',
                                 'Tracking Number',
                                 'Time Stamp',
                                 'Order Reference Number',
                                 'Canceled Shares'])

Order_Delete_index = find_index(['Message Type',
                                 'Stock Locate',
                                 'Tracking Number',
                                 'Time Stamp',
                                 'Order Reference Number'])

Order_Replace_index = find_index(['Message Type',
                                  'Stock Locate',
                                  'Tracking Number',
                                  'Time Stamp',
                                  'Original Order Reference Number',
                                  'New Order Reference Number',
                                  'Shares',
                                  'Price'])

Trade_index = find_index(['Message Type',
                          'Stock Locate',
                          'Tracking Number',
                          'Time Stamp',
                          'Order Reference Number',
                          'Buy/Sell Indicator',
                          'Shares',
                          'Stock',
                          'Price',
                          'Match Number'])

Cross_Trade_index = find_index(['Message Type',
                                'Stock Locate',
                                'Tracking Number',
                                'Time Stamp',
                                'Shares',
                                'Stock',
                                'Cross Price',
                                'Match Number',
                                'Cross Type'])

Broken_Trade_index = find_index(['Message Type',
                                 'Stock Locate',
                                 'Tracking Number',
                                 'Time Stamp',
                                 'Match Number'])

NOII_index = find_index(['Message Type',
                         'Stock Locate',
                         'Tracking Number',
                         'Time Stamp',
                         'Paired Shares',
                         'Imbalance Shares',
                         'Imbalance Direction',
                         'Stock',
                         'Far price',
                         'Near Price',
                         'Current Reference Price',
                         'Cross Type',
                         'Price Variation Indicator'])

RPII_index = find_index(['Message Type',
                         'Stock Locate',
                         'Tracking Number',
                         'Time Stamp',
                         'Stock',
                         'Interest Flag'])

# total line number: 281719135
code_map = pd.read_table('data/grouped/stock_located.txt', sep='\t')
# create a data frame dictionary to store your data frames
#store = pd.HDFStore('data/HDF5/store.h5', "w", complevel=9, complib='zlib')
input = "07292016.NASDAQ_ITCH50"
input_file = "data/" + input
fr = open(input_file, "rb")
stock_locate_index = find_index(['Stock Locate'])[0]
stock_symbol_index = find_index(['Stock'])[0]
chunk_size = 10000
lock = multiprocessing.Lock()
empty_df = pd.DataFrame(index=range(chunk_size), columns=columns)
for COUNTER in trange(1):
    DataFrameDict = {}
    for counter in trange(0, chunk_size):
        byte = fr.read(2)
        if not byte:
            print('Finish Reading(out of byte)')
            break
        message_length = struct.unpack('!H', byte)[0]
        message = fr.read(message_length)
        if not message:
            print('Finish Reading(out of message)')
            break
        messageType = chr(message[0])
        RESULT = ParseMessage(message, messageType)
        store_code = clean_name(find_name(int(RESULT[stock_locate_index])))
        try:
            DataFrameDict[store_code].append(RESULT)
        except:
            DataFrameDict[store_code] = []
            DataFrameDict[store_code].append(RESULT)

    temp_dict = DataFrameDict.copy()
    #del DataFrameDict
    #write_hdf5(temp_dict)

    try:
        IO_process.join()
    except:
        print('No need to wait IO process!\n')
    del DataFrameDict
    IO_process = Process(target=write_hdf5, args=(temp_dict,))
    IO_process.start()

    # write_hdf5(temp_dict)
print('Finish Writing!')
fr.close()

'''
x=[]
start_time = time.time()
for i in range(100000):
    np_array = ['']*56
    index = [34,52,54,53,10]
    p = 0
    for i in index:
        np_array[i] = array[p]
        p+=1
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
for i in range(100000):
    x = np.array([1,2,3]).reshape((1, len(array))
print("--- %s seconds ---" % (time.time() - start_time))
'''
