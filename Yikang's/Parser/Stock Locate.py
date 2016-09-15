import struct
import os
import pandas as pd
from bitstring import BitArray

os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/")
#os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")
def form_list(list):
    for position in range(0,len(list)):
        if type(list[position]) == type(b'byte'):
            list[position] = list[position].decode("utf-8")
            list[position] = list[position].strip()
    return(list)

def Stock_Directory(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sccIcc2scccccIc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)

input_file = "data/07292016.NASDAQ_ITCH50"
output_file = 'data/grouped/stock_located.txt'
fr = open(input_file, "rb")
code_map = pd.DataFrame(columns=['Stock Locate', 'Symbol'])


# order book detail
# fd = open(orderbook_detail_file_name, "w", newline='')

while(True):
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
    if messageType == 'R':
        RESULT = Stock_Directory(message)
        RESULT = form_list(RESULT)
        print(RESULT)
        code_map.loc[len(code_map)] = [str(int(RESULT[1])), RESULT[4]]

fr.close()
code_map.to_csv(output_file, sep='\t', encoding='utf-8', index=False)
