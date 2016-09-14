import pandas as pd
import os
import csv
import sys
from tqdm import *

#os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")


def add_order_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'type': message['x2'],
             'quantity': message['x3'], 'price': message['x5']}
    return (order)


def add_MPID_order_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'type': message['x2'],
             'quantity': message['x3'], 'price': message['x5']}
    return (order)


def order_excuted_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'quantity': message['x2']}
    return (order)


def price_order_excuted_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'quantity': message['x2'],
             'price': message['x5']}
    return (order)


def canel_order_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'quantity': message['x2']}
    return (order)


def delete_order_handler(message):
    order = {'time': message['Time'], 'id': message['x1']}
    return (order)


def replace_order_handler(message):
    order = {'time': message['Time'], 'id': message['x1'], 'newid': message['x2'],
             'quantity': message['x3'], 'price': message['x4']}
    return (order)

def main():
    input_file = sys.argv[1]
    file = input_file.split('.')[0]
    data = pd.read_csv('data/grouped/' + input_file, header=None,
                       names=['EventCode', 'StockLocate', 'Tracking', 'Time',
                              'x1', 'x2', 'x3', 'x4', 'x5', 'x6'])
    order_book = pd.DataFrame(index=range(0, 3))
    price_dt = pd.DataFrame(columns=['Time', 'Price', 'Quantity'])
    price_file_name = "data/price/" + file + "_price_track.txt"

    for iteration in trange(len(data)):
        message = data.iloc[iteration]

        if message.EventCode == 'A':
            information = add_order_handler(message)
            order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                                  information['type']]
        elif message.EventCode == 'F':
            information = add_MPID_order_handler(message)
            order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                                  information['type']]
        elif message.EventCode == 'E':
            information = order_excuted_handler(message)
            order_to_delete = str(information['id'])
            order_price = str(order_book[order_to_delete][1])
            order_book[order_to_delete][0] -= int(float(information['quantity']))
            if order_book[order_to_delete][0] <= 0:
                order_book = order_book.drop(order_to_delete, 1)
            price_dt.loc[len(price_dt)] = [str(information['time']), order_price, int(information['quantity'])]

        elif message.EventCode == 'C':
            information = price_order_excuted_handler(message)
            #    print(information['time'] + ": " + str(information['quantity']) +
            #          " shares of order just executed as price of " + str(information['price']))
            order_to_delete = str(information['id'])
            order_book[order_to_delete][0] -= int(float(information['quantity']))
            if order_book[order_to_delete][0] <= 0:
                order_book = order_book.drop(order_to_delete, 1)
            price_dt.loc[len(price_dt)] = [str(information['time']), information['price'],
                                           int(information['quantity'])]


        elif message.EventCode == 'U':
            information = replace_order_handler(message)
            #    print(str(information['time'])+ ": " + str(information['quantity']) +
            #          " shares of order has been replaced")
            old_order_id = str(information['id'])
            old_type = order_book[old_order_id][2]
            new_order_id = str(information['newid'])
            new_price = str(information['price'])
            new_quantity = int(float(information['quantity']))
            order_book[new_order_id] = [new_quantity, new_price, old_type]
            order_book = order_book.drop(old_order_id, 1)

        elif message.EventCode == 'D':
            information = delete_order_handler(message)
            order_to_delete = str(information['id'])
            order_book = order_book.drop(order_to_delete, 1)

        elif message.EventCode == 'X':
            information = canel_order_handler(message)
            #    print(information['time'] + ": " + str(information['quantity'])
            #        + " shares of order just executed")
            order_to_delete = str(information['id'])
            order_book[order_to_delete][0] -= int(float(information['quantity']))
            if order_book[order_to_delete][0] <= 0:
                order_book = order_book.drop(order_to_delete, 1)

        else:
            print('Wrong Message Type!')
    print("finish writing...")
    price_dt.to_csv(price_file_name, sep='\t', encoding='utf-8')

if __name__ == "__main__":
    print (sys.argv[:])
    main()