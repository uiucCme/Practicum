import struct
import os
import csv
from bitstring import BitArray
from datetime import timedelta

#os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")
def form_list(list):
    for position in range(0,len(list)):
        if type(list[position]) == type(b'byte'):
            list[position] = list[position].decode("utf-8")
            list[position] = list[position].strip()
    return(list)


def System_Event_Message(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!c", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[-1] = chr(ord(array[-1]))
    return(array)


def Stock_Directory(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sccIcc2scccccIc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Stock_Trading(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8scc4s", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Reg_SHO(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Market_Participant_Position(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!4s8sccc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def MWCB_Decline(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def MWCB_Status(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!c", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def IPOUpdate(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sIcI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Add_Order(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcI8sI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    return(array)


def Add_MPID_Order(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcI8sI4s", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    return(array)


def Excueted_Order(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QIQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Excueted_Price_Order(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QIQcI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[8] = array[8] / 10000
    return(array)


def Order_Cancel(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QI", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Order_Delete(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Order_Replace(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQII", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    array[7] = array[7] / 10000
    return(array)


def Trade(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QcIQIQ", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Cross_Trade(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q8sIQc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def Broken_Trade(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!Q", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def NOII(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!QQc8sIIIcc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def RPII(message):
    array1 = [chr(message[0])] + list(struct.unpack("!HH", message[1:5]))
    b = BitArray(bytes=message[5:11])
    array2 = list(struct.unpack("!8sc", message[11:len(message)]))
    array = array1 + b.unpack('uintbe:48') + array2
    return(array)


def ParseMessage(message, messageType):
    if messageType == 'S':
        return(System_Event_Message(message))
    elif messageType == 'R':
        return(Stock_Directory(message))
    elif messageType == 'H':
        return(Stock_Trading(message))
    elif messageType == 'Y':
        return(Reg_SHO(message))
    elif messageType == 'L':
        return(Market_Participant_Position(message))
    elif messageType == 'V':
        return(MWCB_Decline(message))
    elif messageType == 'W':
        return(MWCB_Status(message))
    elif messageType == 'K':
        return(IPOUpdate(message))
    elif messageType == 'A':
        return(Add_Order(message))
    elif messageType == 'F':
        return(Add_MPID_Order(message))
    elif messageType == 'E':
        return(Excueted_Order(message))
    elif messageType == 'C':
        return(Excueted_Price_Order(message))
    elif messageType == 'X':
        return(Order_Cancel(message))
    elif messageType == 'D':
        return(Order_Delete(message))
    elif messageType == 'U':
        return(Order_Replace(message))
    elif messageType == 'P':
        return(Trade(message))
    elif messageType == 'Q':
        return(Cross_Trade(message))
    elif messageType == 'B':
        return(Broken_Trade(message))
    elif messageType == 'I':
        return(NOII(message))
    elif messageType == 'N':
        return(RPII(message))
    else:
        print('message type not found!')
        return

input = "07292016.NASDAQ_ITCH50"
time_track = '0:00'
useful_event_code = ['A', 'F', 'E', 'C', 'U', 'D', 'X']
input_file = "data/" + input
f = open(input_file, "rb")
# decode_filename = "data/" + input + "output.csv"
orderbook_filename = "data/grouped/" + "SPY.csv"
# resultFile = open(decode_filename, 'w')
resultFile2 = open(orderbook_filename, 'w', newline='')
while(True):
    byte = f.read(2)
    if not byte:
        print('Finish Reading(out of byte)')
        break
    message_length = struct.unpack('!H', byte)[0]
    message = f.read(message_length)
    if not message:
        print('Finish Reading(out of message)')
        break
    messageType = chr(message[0])
    RESULT = ParseMessage(message, messageType)
    RESULT = form_list(RESULT)
    RESULT[3] =  str(timedelta(seconds=RESULT[3]/1e+9))
    curr_time_split = RESULT[3].split(":")
    curr_time = curr_time_split[0] + ":" + curr_time_split[1]
    if curr_time != time_track:
        print (RESULT[3])
        #print (RESULT)
        time_track = curr_time
    if any(code == messageType for code in useful_event_code):
        if RESULT[1] == 7030:
            #print(RESULT)
            wr2 = csv.writer(resultFile2, dialect='excel')
            wr2.writerow(RESULT)
        # wr = csv.writer(resultFile, dialect='excel')
        # wr.writerow(RESULT)
f.close()
#resultFile.close()
resultFile2.close()

'''
p=Process(target=main(inputfile))
p.start()

'''