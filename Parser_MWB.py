# -*- coding: utf-8 -*-
"""
Created on Mon Sep  5 11:56:58 2016

@author: markbright
"""
import os
import struct
import csv
import sys

os.chdir('/Users/markbright/Documents/MSFE/Practicum/')

#os.getcwd() 


## WORKING
i=1
f = open('07292016.NASDAQ_ITCH50', 'rb')
 #print("i value: ",i)
data = ["Message Type", "Stock Locate", "Tracking Number", "Timestamp"]
with open('MBright_Parser_All.csv', 'w') as a:
    wtr = csv.writer(a)  # use the file as the argument to writer
    wtr.writerow(data)
    while True:

        #a.close()
        ch=f.read(1) # reads specified length, NOT position
        i+=1
        #print("Length: ",len(ch))
        #test_case = struct.unpack(">c",char) #2 bytes
        if ch==b'S':
            #print(ch)
            #print('********Winner******')
            ch2=f.read(11)
            #print("Length of ch: ",len(ch))
            ch2= struct.unpack("!HH6sc", ch2)
            ch3=int.from_bytes(ch2[2], byteorder='big')
            wtr.writerow([ch.decode("utf-8"),ch2[0],ch2[1],ch3,ch2[3].decode("utf-8")])
            #print(ch,ch2[0],ch2[1],ch3,ch2[3])
            i= i+len("!HH6sc")
            if i==100:
                break
        if ch==b'R':
            #print("Found an R**")
            R_before = f.read(38)
            #R_Values= struct.unpack("!HH6s8ccc2Hcc2cccccc2Hc",R_before)
            R_Values= struct.unpack("!HH6s8cccIcc2ccccccIc",R_before)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            # USE BELOW LOOP FOR 8c STOCK NAMES
            stock_name=''
            for x in R_Values[3:10]:
                stock_name=stock_name+x.decode("utf-8")
            c_sub = R_Values[17]+R_Values[18]
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,stock_name,R_Values[11].decode("utf-8"),R_Values[12].decode("utf-8"),R_Values[13],R_Values[14].decode("utf-8"),R_Values[15].decode("utf-8"),R_Values[16].decode("utf-8"),c_sub.decode("utf-8"),R_Values[19].decode("utf-8"),R_Values[20].decode("utf-8"),R_Values[21].decode("utf-8"),R_Values[22].decode("utf-8"),R_Values[23],R_Values[24].decode("utf-8")])
        if ch==b'H':
            #print("Found an H**")
            R_before = f.read(24)
            R_Values= struct.unpack("!HH6s8ccc4c",R_before)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            stock_name=''
            for x in R_Values[3:10]:
                stock_name=stock_name+x.decode("utf-8")
            reason=''
            for y in R_Values[3:10]:
               reason=reason+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,stock_name,R_Values[11].decode("utf-8"),R_Values[12].decode("utf-8"),reason])
            #break
        if ch==b'Y':
            #print("Found an Y**")
            R_before = f.read(19)
            R_Values= struct.unpack("!HH6s8cc",R_before)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            stock_name=''
            for x in R_Values[3:10]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,stock_name,R_Values[11].decode("utf-8")])
        if ch==b'L':
            #print("Found an L*********")
            R_before = f.read(25)
            R_Values= struct.unpack("!HH6s4c8cccc",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            MPID=''
            for y in R_Values[3:7]:
                MPID=MPID+y.decode("utf-8")
            stock_name=''
            for x in R_Values[7:14]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,MPID,stock_name,R_Values[15].decode("utf-8"),R_Values[16].decode("utf-8"),R_Values[17].decode("utf-8")])
        
        if ch==b'V':
            #print("*********Found an V*********")
            R_before = f.read(34)
            R_Values= struct.unpack("!HH6sQQQ",R_before)
            struct.calcsize("!HH6s8cIcL")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3]/10000000,R_Values[4]/10000000,R_Values[5]/10000000])
            #break
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
        if ch==b'W':
            #print("*********Found an W*********")
            R_before = f.read(11)
            R_Values= struct.unpack("!HH6sc",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3].decode("utf-8")])
            #break
        if ch==b'K':
            #print("*********Found an K*********")
            R_before = f.read(27)
            R_Values= struct.unpack("!HH6s8cIcL",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            stock_name=''
            for x in R_Values[3:10]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,stock_name,R_Values[11],R_Values[12].decode("utf-8"),R_Values[13]/10000])
            #break
        if ch==b'A':
            #print("*********Found an A*********")
            R_before = f.read(35)
            R_Values= struct.unpack("!HH6sQcI8cI",R_before)
            #test_decimal = R_Values2[32:]
            #convert = Decimal(test_decimal)
            #print("Decimal: ",convert)
            struct.calcsize("!HH6sQcI8cL")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            stock_name=''
            for x in R_Values[6:13]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4].decode("utf-8"),R_Values[5],stock_name,R_Values[14]/10000])
                
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:-2],R_Values[-1]/10000)
            #break
        if ch==b'F':
            #print("*********Found an F*********")
            R_before = f.read(39)
            R_Values= struct.unpack("!HH6sQcI8cL4c",R_before)
            struct.calcsize("!HH6sQIQ")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            stock_name=''
            for x in R_Values[6:13]:
                stock_name=stock_name+x.decode("utf-8")
            attrib=''
            for y in R_Values[15:]:
                attrib=attrib+y.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4].decode("utf-8"),R_Values[5],stock_name,R_Values[14]/10000,attrib])
            #break
        if ch==b'E':
            #print("*********Found an E*********")
            R_before = f.read(30)
            R_Values= struct.unpack("!HH6sQIQ",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4],R_Values[5]])
            #break
        if ch==b'C':
            #print("*********Found an C*********")
            R_before = f.read(35)
            R_Values= struct.unpack("!HH6sQIQcL",R_before)
            struct.calcsize("!HH6sQIQcL")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4],R_Values[5],R_Values[6].decode("utf-8"),R_Values[7]/10000])
            #break
        if ch==b'X':
            #print("*********Found an X*********")
            R_before = f.read(22)
            R_Values= struct.unpack("!HH6sQI",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4]])
            #break
        if ch==b'D':
            #print("*********Found an D*********")
            R_before = f.read(18)
            R_Values= struct.unpack("!HH6sQ",R_before)
            struct.calcsize("!HH6sQ")
            #struct.calcsize("!HH6sL")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3]])
            #break
        if ch==b'U':
            #print("*********Found an U*********")
            R_before = f.read(34)
            R_Values= struct.unpack("!HH6sQQIL",R_before)
            struct.calcsize("!HH6sQ")
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:]) 
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4],R_Values[5],R_Values[6]/10000])
            #break
        if ch==b'P':
            #print("*********Found an P*********")
            R_before = f.read(43)
            R_Values= struct.unpack("!HH6sQcI8cLQ",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            stock_name=''
            for x in R_Values[6:13]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4].decode("utf-8"),R_Values[5],stock_name,R_Values[14]/10000,R_Values[15]])
            #break
        if ch==b'Q':
            #print("*********Found an Q*********")
            R_before = f.read(39)
            R_Values= struct.unpack("!HH6sQ8cLQc",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            stock_name=''
            for x in R_Values[4:11]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],stock_name,R_Values[12]/10000,R_Values[13],R_Values[14].decode("utf-8")])
            #break
        if ch==b'B':
            #print("*********Found an B*********")
            R_before = f.read(18)
            R_Values= struct.unpack("!HH6sQ",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3]])
            #break
        if ch==b'I':
            #print("*********Found an I*********")
            R_before = f.read(49)
            R_Values= struct.unpack("!HH6sQQc8cLLLcc",R_before)
            #print(R_Values)
            ch3=int.from_bytes(R_Values[2], byteorder='big')
            #print(ch,R_Values[0],R_Values[1],ch3,R_Values[3:])
            stock_name=''
            for x in R_Values[6:13]:
                stock_name=stock_name+x.decode("utf-8")
            wtr.writerow([ch.decode("utf-8"),R_Values[0],R_Values[1],ch3,R_Values[3],R_Values[4],R_Values[5].decode("utf"),stock_name,R_Values[14]/10000,R_Values[15]/10000,R_Values[16]/10000,R_Values[17].decode("utf"),R_Values[18].decode("utf")])
            #break
        if not ch: 
            a.close()
            print("ENDING")
            break
        #print("ch: ",ch)
    #    if i == 10000:
    #        print("about to break")
    #        break
    
        ## WORKING
    
        
