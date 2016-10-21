import pandas as pd
import os
import csv
from tqdm import *

os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
# os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")


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


def output_dict(time, type, bid, ask):
    array_value = []
    array_value.append(time)
    array_value.append(type)
    for key in bid.columns.values.tolist():
        array_value.append(key)
        array_value.append(bid[key][0])
    array_value.append('||')
    for key in ask.columns.values.tolist():
        array_value.append(key)
        array_value.append(ask[key][0])
    return (array_value)


file = 'SPY'
data = pd.read_csv('data/grouped/' + file + '.csv', header=None,
                   names=['EventCode', 'StockLocate', 'Tracking', 'Time',
                          'x1', 'x2', 'x3', 'x4', 'x5', 'x6'])
bid = pd.DataFrame(index=range(0, 1))
ask = pd.DataFrame(index=range(0, 1))
order_book = pd.DataFrame(index=range(0, 3))

orderbook_file_name = "I:/" + file + "_order_book.txt"
orderbook_detail_file_name = "data/order_book/" + file + "detail.txt"
f = open(orderbook_file_name, "w", newline='')
fd = open(orderbook_detail_file_name, "w", newline='')

for iteration in tqdm(range(0, len(data))):
    message = data.iloc[iteration]

    if message.EventCode == 'A':
        information = add_order_handler(message)
        if information['type'] == 'B':
            if str(information['price']) in bid.columns.values.tolist():
                bid[str(information['price'])] += int(float(information['quantity']))
            else:
                bid[str(information['price'])] = int(float(information['quantity']))
        else:
            if str(information['price']) in ask.columns.values.tolist():
                ask[str(information['price'])] += int(float(information['quantity']))
            else:
                ask[str(information['price'])] = int(float(information['quantity']))
        order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                              information['type']]
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been added at price " + str(order_book[str(information['id'])][1]))


    elif message.EventCode == 'F':
        information = add_MPID_order_handler(message)
        if information['type'] == 'B':
            if str(information['price']) in bid.columns.values.tolist():
                bid[str(information['price'])] += int(float(information['quantity']))
            else:
                bid[str(information['price'])] = int(float(information['quantity']))
        else:
            if str(information['price']) in ask.columns.values.tolist():
                ask[str(information['price'])] += int(float(information['quantity']))
            else:
                ask[str(information['price'])] = int(float(information['quantity']))
        order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                              information['type']]
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been added at price " + str(order_book[str(information['id'])][1]))

    elif message.EventCode == 'E':
        information = order_excuted_handler(message)
        order_to_delete = str(information['id'])
        order_price = str(order_book[order_to_delete][1])
        order_type = str(order_book[order_to_delete][2])
        #print(information['time'] + ": " + str(information['quantity']) +
        #     " shares of order just executed at price " + str(order_book[str(information['id'])][1]))
        if order_type == 'B':
            bid[order_price] -= int(float(information['quantity']))
            if int(bid[order_price]) <= 0:
                bid = bid.drop(order_price, 1)
        else:
            ask[order_price] -= int(float(information['quantity']))
            if int(ask[order_price]) <= 0:
                ask = ask.drop(order_price, 1)
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just executed at price " + str(order_book[str(information['id'])][1]))
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)

    elif message.EventCode == 'C':
        information = price_order_excuted_handler(message)
        order_to_delete = str(information['id'])
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        order_price = str(order_book[order_to_delete][1])
        order_type = str(order_book[order_to_delete][2])
        if order_type == 'B':
            bid[order_price] -= int(float(information['quantity']))
            if int(bid[order_price]) <= 0:
                bid = bid.drop(order_price, 1)
        else:
            ask[order_price] -= int(float(information['quantity']))
            if int(ask[order_price]) <= 0:
                ask = ask.drop(order_price, 1)
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just executed at price " + str(information['price']))


    elif message.EventCode == 'U':
        information = replace_order_handler(message)
        #    print(str(information['time'])+ ": " + str(information['quantity']) +
        #          " shares of order has been replaced")
        old_order_id = str(information['id'])
        old_quantity = int(order_book[old_order_id][0])
        old_price = str(order_book[old_order_id][1])
        old_type = str(order_book[old_order_id][2])
        new_order_id = str(information['newid'])
        new_price = str(information['price'])
        new_quantity = int(float(information['quantity']))
        order_book[str(information['newid'])] = [new_quantity, new_price, old_type]
        order_book = order_book.drop(old_order_id, 1)

        if old_type == 'B':
            bid[old_price] -= old_quantity
            if int(bid[old_price]) <= 0:
                bid = bid.drop(old_price, 1)
            if str(new_price) in bid.columns.values.tolist():
                bid[new_price] += new_quantity
            else:
                bid[new_price] = new_quantity
        else:
            ask[old_price] -= old_quantity
            if int(ask[old_price]) <= 0:
                ask = ask.drop(old_price, 1)
            if str(new_price) in ask.columns.values.tolist():
                ask[new_price] += new_quantity
            else:
                ask[new_price] = new_quantity

        fd.write(str(information['time']) + ": " + str(old_quantity) +
                 " shares of order at price " + str(old_price) +
                 " has been replaced as " + str(new_quantity) + " shares of order at price " +
                 str(new_price))

    elif message.EventCode == 'D':
        information = delete_order_handler(message)
        order_to_delete = str(information['id'])
        delete_quantity = order_book[order_to_delete][0]
        order_price = str(order_book[order_to_delete][1])
        order_type = str(order_book[order_to_delete][2])
        if order_type == 'B':
            bid[order_price] -= delete_quantity
            if int(bid[order_price]) <= 0:
                bid = bid.drop(order_price, 1)
        else:
            ask[order_price] -= delete_quantity
            if int(ask[order_price]) <= 0:
                ask = ask.drop(order_price, 1)
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been deleted at price " + str(order_book[str(information['id'])][1]))
        order_book = order_book.drop(order_to_delete, 1)

    elif message.EventCode == 'X':
        information = canel_order_handler(message)
        #    print(information['time'] + ": " + str(information['quantity'])
        #        + " shares of order just executed")
        order_to_delete = str(information['id'])
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        order_price = str(order_book[order_to_delete][1])
        order_type = str(order_book[order_to_delete][2])
        if order_type == 'B':
            bid[order_price] -= int(float(information['quantity']))
            if int(bid[order_price]) <= 0:
                bid = bid.drop(order_price, 1)
        else:
            ask[order_price] -= int(float(information['quantity']))
            if int(ask[order_price]) <= 0:
                ask = ask.drop(order_price, 1)
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just canceled  at price " + str(order_book[str(information['id'])][1]))
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)

    else:
        print('Wrong Message Type!')

    type = message.EventCode
    bid = bid.reindex_axis([str(j) for j in sorted(float(i) for i in bid.columns)], axis=1)
    ask = ask.reindex_axis([str(j) for j in sorted(float(i) for i in ask.columns)], axis=1)
    # print()
    wr = csv.writer(f, dialect='excel')
    fd.write('\n')
    # print(output_dict(information['time'], bid, ask))
    wr.writerow(output_dict(information['time'], type, bid, ask))

print("finish writing...")
f.close()
fd.close()
