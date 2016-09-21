import struct
import os
import pandas as pd
import numpy as np
from tqdm import *
from datetime import timedelta
import timeit


#os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")

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
    np_array = np.array(array).reshape((1, len(array)))
    array_df = pd.DataFrame(form_list(np_array),
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Event Code'])
    return (array_df)


def Stock_Directory(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8sccIcc2scccccIc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
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
    return (array_df)


def Stock_Trading(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8scc4s", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Stock',
                                     'Trading State',
                                     'Reserved',
                                     'Reason'])
    return (array_df)


def Reg_SHO(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Stock',
                                     'Reg SHO Action'])
    return (array_df)


def Market_Participant_Position(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s4s8sccc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'MPID',
                                     'Stock',
                                     'Primary Market Maker',
                                     'Market Maker Mode',
                                     'Market Participant State'])
    return (array_df)


def MWCB_Decline(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQQQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Level 1',
                                     'Level 2',
                                     'Level 3'])
    return (array_df)


def MWCB_Status(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Bereached Level'])
    return (array_df)


def IPOUpdate(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8cIcI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Stock',
                                     'IPO Quotation Release Time',
                                     'IPO Quotation Release Qualifier',
                                     'IPO Price'])
    return (array_df)


def Add_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQcI8cI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Buy/Sell Indicator',
                                     'Shares',
                                     'Stock',
                                     'Price'])
    return (array_df)


def Add_MPID_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQcI8sI4s", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Buy/Sell Indicator',
                                     'Shares',
                                     'Stock',
                                     'Price',
                                     'Attribution'])
    return (array_df)


def Excueted_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQIQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Executed Shares',
                                     'Match Number'])
    return (array_df)


def Excueted_Price_Order(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQIQcI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Executed Shares',
                                     'Match Number',
                                     'Printable',
                                     'Execution Price'])
    return (array_df)


def Order_Cancel(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQI", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Canceled Shares'])
    return (array_df)


def Order_Delete(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number'])
    return (array_df)


def Order_Replace(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQQII", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    array[7] = array[7] / 10000
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Original Order Reference Number',
                                     'New Order Reference Number',
                                     'Shares',
                                     'Price'])
    return (array_df)


def Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQcIQIQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number',
                                     'Buy/Sell Indicator',
                                     'Shares',
                                     'Stock',
                                     'Price',
                                     'Match Number'])
    return (array_df)


def Cross_Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ8sIQc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Shares',
                                     'Stock',
                                     'Cross Price',
                                     'Match Number',
                                     'Cross Type'])
    return (array_df)


def Broken_Trade(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQ", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Match Number'])
    return (array_df)


def NOII(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6sQQc8sIIIcc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
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
    return (array_df)


def RPII(message):
    array = [chr(message[0])] + list(struct.unpack("!HH6s8sc", message[1:]))
    array[3] = int.from_bytes(array[3], byteorder='big')
    np_array = np.array(form_list(array)).reshape((1, len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Stock',
                                     'Interest Flag'])
    return (array_df)


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


# total line number: 281719135
code_map = pd.read_table('data/grouped/stock_located.txt', sep='\t')
# create a data frame dictionary to store your data frames
store = pd.HDFStore('data/HDF5/store.h5',"w", complevel=9, complib='zlib')
input = "07292016.NASDAQ_ITCH50"
input_file = "data/" + input
fr = open(input_file, "rb")
index = range(200000)
columns = ['Level 1', 'Issue Subtype', 'MPID',
           'Market Maker Mode', 'IPO Quotation Release Qualifier',
           'Current Reference Price', 'Round Lot Size', 'Paired Shares',
           'Message Type', 'Price Variation Indicator', 'Cross Type',
           'Tracing Number', 'Authenticity', 'Far price', 'Level 3',
           'Interest Flag', 'Order Reference Number', 'Reason', 'Cross Price',
           'Trading State', 'Bereached Level', 'Inverse Indicator',
           'ETP Leverage Factor', 'IPO Flag', 'LUID Reference Price Tier',
           'Near Price', 'Time Stamp', 'Round Lots Only', 'Level 2',
           'Imbalance Shares', 'Reserved', 'Executed Shares',
           'IPO Quotation Release Time', 'Reg SHO Action',
           'Original Order Reference Number', 'ETP Flag', 'IPO Price',
           'Buy/Sell Indicator', 'Shares', 'Canceled Shares', 'Stock',
           'New Order Reference Number', 'Market Category',
           'Short Sale Threshold Indicator', 'Financial Status Indicator',
           'Printable', 'Price', 'Market Participant State',
           'Imbalance Direction', 'Attribution', 'Execution Price',
           'Primary Market Maker', 'Event Code', 'Issue Classification',
           'Match Number', 'Stock Locate']

empty_df = pd.DataFrame(index=index, columns=columns)

for COUNTER in trange(int(281719135 / 200000) + 1):
    DataFrameDict = {}
    for counter in trange(0, 200000):
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
        store_code = find_name(int(RESULT['Stock Locate'][0])).replace("-", "")
        try:
            DataFrameDict[store_code] = DataFrameDict[store_code].append(RESULT)
        except:
            DataFrameDict[store_code] = RESULT

    for key in tqdm(DataFrameDict.keys()):
        try:
            store[key] = store[key].append(DataFrameDict[key])
        except:
            store[key] = DataFrameDict[key]
    print(str(timedelta(seconds=store['SPY'] / 1e+9)))
    del DataFrameDict

store.close()
fr.close()

'''
x=[]
start_time = time.time()
for i in range(100000):
    x.append([1,2,3])
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
for i in range(100000):
    x = np.array([1,2,3]).reshape((1, len(array))
print("--- %s seconds ---" % (time.time() - start_time))
'''

