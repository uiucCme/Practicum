import struct
import os
import pandas as pd
import numpy as np
from bitstring import BitArray
from datetime import timedelta

os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
# os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")

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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!c", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[-1] = chr(ord(array[-1]))
    np_array = np.array(form_list(array)).reshape((1,len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Event Code'])
    return (array_df)


def Stock_Directory(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sccIcc2scccccIc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8scc4s", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!4s8sccc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!c", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Bereached Level'])
    return (array_df)


def IPOUpdate(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sIcI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcI8sI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcI8sI4s", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QIQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QIQcI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Order Reference Number'])
    return (array_df)



def Order_Replace(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQII", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[7] = array[7] / 10000
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcIQIQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q8sIQc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    np_array = np.array(form_list(array)).reshape((1,len(array)))
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array_df = pd.DataFrame(np_array,
                            index=range(1),
                            columns=['Message Type',
                                     'Stock Locate',
                                     'Tracing Number',
                                     'Time Stamp',
                                     'Match Number'])
    return (array_df)


def NOII(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQc8sIIIcc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
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
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
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


code_map = pd.read_table('data/grouped/stock_located.txt', sep='\t')
store = pd.HDFStore('store.h5')
input = "07292016.NASDAQ_ITCH50"
input_file = "data/" + input
fr = open(input_file, "rb")
while (True):
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
    print(RESULT['Message Type'])
    try:
        store[store_code] = pd.merge(store[store_code], RESULT, how='outer')
    except:
        store[store_code] = RESULT


fr.close()
