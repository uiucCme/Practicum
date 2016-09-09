import pandas as pd
import os
os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
input = "data/order_book/SPY_order_book.txt"
output = "data/order_book/SPY order book first 10000 lines.txt"
I = open(input, 'r')
O = open(output, 'w')
i = 0
count = 1
for line in I:
    # Display data retrieved
    print(count, ": ", line)
    O.write(line)
    # add to count sequence
    count += 1
    if count == 10000:
        break

I.close()
O.close()
