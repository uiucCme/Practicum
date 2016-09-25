import struct
import os
import pandas as pd
import numpy as np
import multiprocessing
from multiprocessing import Process
from tqdm import *
import threading
import h5py
from datetime import timedelta
import time
# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")

def get_data(stock='NA', type_needed='NA'):
    if stock == 'NA':
        get_df = pd.read_hdf('data/HDF5/store.h5',
                             'RAW').replace("", np.nan)
    else:
        get_df = pd.read_hdf('data/HDF5/store.h5',
                             'RAW',
                             where='Stock == %s' % stock).replace("", np.nan)
    if type_needed != 'NA':
        if(type(type_needed) == type('Str')):
            type_needed = [type_needed]
        get_df = get_df[get_df['Message_Type'].isin(type_needed)]
    get_df.dropna(1, how='all', inplace=True)
    return (get_df.reset_index(drop=True))



def clean_name(input_name):
    input_name = input_name.replace('.', "")
    input_name = input_name.replace("-", "m")
    input_name = input_name.replace('+', 'p').replace('*', "s")
    input_name = input_name.replace('=', 'e').replace('#', "pp")
    return(input_name)


def write_hdf5(list_name):
    #lock.acquire()
    write_df = pd.DataFrame(list_name, columns=columns, dtype='object').convert_objects()
    store = pd.HDFStore('data/HDF5/store.h5', mode='a')
    #write_df.to_hdf('data/HDF5/store.h5', 'RAW', append=True)
    store.append('RAW', write_df, data_columns=True, index=False)
    store.close()
    #lock.release()
    print('\nFinished This Chunck\n')

def find_index(a):
    result = []
    for x in a:
        result.append(columns.index(x))
    return (result)


def find_name(code):
    return (code_map['Symbol'][code])


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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
    complete_array = base_list.copy()
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
        print('Message_Type not found!')
        return


columns = ['Attribution', 'Authenticity', 'Breached_Level',                                         #0
           'Buy_Sell_Indicator', 'Canceled_Shares', 'Cross_Price',                                  #1
           'Cross_Type', 'Current_Reference_Price', 'ETP_Flag',                                     #2
           'ETP_Leverage_Factor', 'Event_Code', 'Executed_Shares',                                  #3
           'Execution_Price', 'Far_price', 'Financial_Status_Indicator',                            #4
           'IPO_Flag', 'IPO_Price', 'IPO_Quotation_Release_Qualifier',                              #5
           'IPO_Quotation_Release_Time', 'Imbalance_Direction', 'Imbalance_Shares',                 #6
           'Interest_Flag', 'Inverse_Indicator', 'Issue_Classification',                            #7
           'Issue_Subtype', 'LULD_Reference_Price_Tier', 'Level_1',                                 #8
           'Level_2', 'Level_3', 'MPID',
           'Market_Category', 'Market_Maker_Mode', 'Market_Participant_State',
           'Match_Number', 'Message_Type', 'Near_Price',
           'New_Order_Reference_Number', 'Order_Reference_Number', 'Original_Order_Reference_Number',
           'Paired_Shares', 'Price', 'Price_Variation_Indicator',
           'Primary_Market_Maker', 'Printable', 'Reason',
           'Reg_SHO_Action', 'Reserved', 'Round_Lot_Size',
           'Round_Lots_Only', 'Shares', 'Short_Sale_Threshold_Indicator',
           'Stock', 'Stock_Locate', 'Time_Stamp',
           'Tracking_Number', 'Trading_State']

base_list = [""]*3 + \
            [""] + [np.nan]*2 \
            + [""] + [np.nan] +[""]\
            +[np.nan] + [""] + [np.nan]*3 + [""]*2 \
            + [np.nan]*1 + [""] + [np.nan] + [""] + [np.nan] + [""]*5 +[np.nan]*3 + [""]*4 + [np.nan] \
            + [""] + [np.nan]*6 + [""]*6 + [np.nan] + [""] + [np.nan] + [""]*2 + [np.nan]*3 + [""]




System_Event_Message_index = find_index(['Message_Type',
                                         'Stock_Locate',
                                         'Tracking_Number',
                                         'Time_Stamp',
                                         'Event_Code'])

Stock_Directory_index = find_index(['Message_Type',
                                    'Stock_Locate',
                                    'Tracking_Number',
                                    'Time_Stamp',
                                    'Stock',
                                    'Market_Category',
                                    'Financial_Status_Indicator',
                                    'Round_Lot_Size',
                                    'Round_Lots_Only',
                                    'Issue_Classification',
                                    'Issue_Subtype',
                                    'Authenticity',
                                    'Short_Sale_Threshold_Indicator',
                                    'IPO_Flag',
                                    'LULD_Reference_Price_Tier',
                                    'ETP_Flag',
                                    'ETP_Leverage_Factor',
                                    'Inverse_Indicator'])

Stock_Trading_index = find_index(['Message_Type',
                                  'Stock_Locate',
                                  'Tracking_Number',
                                  'Time_Stamp',
                                  'Stock',
                                  'Trading_State',
                                  'Reserved',
                                  'Reason'])

Reg_SHO_index = find_index(['Message_Type',
                            'Stock_Locate',
                            'Tracking_Number',
                            'Time_Stamp',
                            'Stock',
                            'Reg_SHO_Action'])

Market_Participant_Position_index = find_index(['Message_Type',
                                                'Stock_Locate',
                                                'Tracking_Number',
                                                'Time_Stamp',
                                                'MPID',
                                                'Stock',
                                                'Primary_Market_Maker',
                                                'Market_Maker_Mode',
                                                'Market_Participant_State'])

MWCB_Decline_index = find_index(['Message_Type',
                                 'Stock_Locate',
                                 'Tracking_Number',
                                 'Time_Stamp',
                                 'Level_1',
                                 'Level_2',
                                 'Level_3'])

MWCB_Status_index = find_index(['Message_Type',
                                'Stock_Locate',
                                'Tracking_Number',
                                'Time_Stamp',
                                'Breached_Level'])

IPOUpdate_index = find_index(['Message_Type',
                              'Stock_Locate',
                              'Tracking_Number',
                              'Time_Stamp',
                              'Stock',
                              'IPO_Quotation_Release_Time',
                              'IPO_Quotation_Release_Qualifier',
                              'IPO_Price'])

Add_Order_index = find_index(['Message_Type',
                              'Stock_Locate',
                              'Tracking_Number',
                              'Time_Stamp',
                              'Order_Reference_Number',
                              'Buy_Sell_Indicator',
                              'Shares',
                              'Stock',
                              'Price'])

Add_MPID_Order_index = find_index(['Message_Type',
                                   'Stock_Locate',
                                   'Tracking_Number',
                                   'Time_Stamp',
                                   'Order_Reference_Number',
                                   'Buy_Sell_Indicator',
                                   'Shares',
                                   'Stock',
                                   'Price',
                                   'Attribution'])

Excueted_Order_index = find_index(['Message_Type',
                                   'Stock_Locate',
                                   'Tracking_Number',
                                   'Time_Stamp',
                                   'Order_Reference_Number',
                                   'Executed_Shares',
                                   'Match_Number'])

Excueted_Price_Order_index = find_index(['Message_Type',
                                         'Stock_Locate',
                                         'Tracking_Number',
                                         'Time_Stamp',
                                         'Order_Reference_Number',
                                         'Executed_Shares',
                                         'Match_Number',
                                         'Printable',
                                         'Execution_Price'])

Order_Cancel_index = find_index(['Message_Type',
                                 'Stock_Locate',
                                 'Tracking_Number',
                                 'Time_Stamp',
                                 'Order_Reference_Number',
                                 'Canceled_Shares'])

Order_Delete_index = find_index(['Message_Type',
                                 'Stock_Locate',
                                 'Tracking_Number',
                                 'Time_Stamp',
                                 'Order_Reference_Number'])

Order_Replace_index = find_index(['Message_Type',
                                  'Stock_Locate',
                                  'Tracking_Number',
                                  'Time_Stamp',
                                  'Original_Order_Reference_Number',
                                  'New_Order_Reference_Number',
                                  'Shares',
                                  'Price'])

Trade_index = find_index(['Message_Type',
                          'Stock_Locate',
                          'Tracking_Number',
                          'Time_Stamp',
                          'Order_Reference_Number',
                          'Buy_Sell_Indicator',
                          'Shares',
                          'Stock',
                          'Price',
                          'Match_Number'])

Cross_Trade_index = find_index(['Message_Type',
                                'Stock_Locate',
                                'Tracking_Number',
                                'Time_Stamp',
                                'Shares',
                                'Stock',
                                'Cross_Price',
                                'Match_Number',
                                'Cross_Type'])

Broken_Trade_index = find_index(['Message_Type',
                                 'Stock_Locate',
                                 'Tracking_Number',
                                 'Time_Stamp',
                                 'Match_Number'])

NOII_index = find_index(['Message_Type',
                         'Stock_Locate',
                         'Tracking_Number',
                         'Time_Stamp',
                         'Paired_Shares',
                         'Imbalance_Shares',
                         'Imbalance_Direction',
                         'Stock',
                         'Far_price',
                         'Near_Price',
                         'Current_Reference_Price',
                         'Cross_Type',
                         'Price_Variation_Indicator'])

RPII_index = find_index(['Message_Type',
                         'Stock_Locate',
                         'Tracking_Number',
                         'Time_Stamp',
                         'Stock',
                         'Interest_Flag'])


# total line number: 281719135
code_map = pd.read_table('data/grouped/stock_located.txt', sep='\t', index_col=0).to_dict()
# create a data frame dictionary to store your data frames
#store = pd.HDFStore('data/HDF5/store.h5', "w", complevel=9, complib='zlib')
input = "07292016.NASDAQ_ITCH50"
input_file = "data/" + input
fr = open(input_file, "rb")
stock_locate_index = find_index(['Stock_Locate'])[0]
stock_symbol_index = find_index(['Stock'])[0]
chunk_size = 5000000
initialize_dataframe = True
lock = multiprocessing.Lock()
for COUNTER in trange(int(281719135/chunk_size)+1):
    temp_list = []
    for counter in range(0, chunk_size):
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
        RESULT[stock_symbol_index] = store_code
        temp_list.append(RESULT)

    try:
        IO_process.join()
    except:
        print('\nNo need to wait IO process!\n')
    if(initialize_dataframe):
        temp_df = pd.DataFrame(temp_list, columns=columns)
        '''
        np_temp_list = np.array(temp_list, dtype="S10")
        f = h5py.File('data/HDF5/store.h5', 'w')
        store = f.create_dataset('RAW', data=np_temp_list, maxshape=(chunk_size, 56))
        '''
        print('\nopen hdf5 file...\n')
        store = pd.HDFStore('data/HDF5/store.h5', mode='w')
        print('\nappending hdf5 file...\n')
        store.append('RAW', temp_df, min_itemsize=30, data_columns=True, index=False)
        print('\nFinished appending!\n')
        store.close()

        initialize_dataframe = False
    else:
        #IO_process = threading.Thread(target=write_hdf5, args=(temp_list,))
        #IO_process.start()
        write_hdf5(temp_list)
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
