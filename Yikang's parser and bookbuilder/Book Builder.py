import pandas as pd
import os
import csv

#os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")
file = 'SPY'
data = pd.read_csv('data/grouped/' + file + '.csv', header=None,
                   names=['EventCode', 'StockLocate', 'Tracking', 'Time',
                          'x1', 'x2', 'x3', 'x4', 'x5', 'x6'])
print(len(data))


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


bid = pd.DataFrame(index=range(0, 1))
ask = pd.DataFrame(index=range(0, 1))
order_book = pd.DataFrame()
(str(j) for j in sorted(float(i) for i in bid.columns))
bid.reindex_axis([str(j) for j in sorted(float(i) for i in bid.columns)], axis=1)
ask.reindex_axis([str(j) for j in sorted(float(i) for i in ask.columns)], axis=1)

orderbook_file_name = "data/order_book/" + file + "_order_book.txt"
orderbook_detail_file_name = "data/order_book/" + file + "detail.txt"
f = open(orderbook_file_name, "w", newline='')
fd = open(orderbook_detail_file_name, "w", newline='')

for iteration in range(0, len(data)):
    message = data.iloc[iteration]

    if message.EventCode == 'A':
        information = add_order_handler(message)
        if information['type'] == 'B':
            if any(code == str(information['price']) for code in bid.columns.values.tolist()):
                bid[str(information['price'])] += int(float(information['quantity']))
            else:
                bid[str(information['price'])] = int(float(information['quantity']))
        else:
            if any(code == str(information['price']) for code in ask.columns.values.tolist()):
                ask[str(information['price'])] += int(float(information['quantity']))
            else:
                ask[str(information['price'])] = int(float(information['quantity']))
        order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                              information['type']]
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been added at price " + str(order_book[str(information['id'])][1]))
        type = 'Order Added'


    elif message.EventCode == 'F':
        information = add_MPID_order_handler(message)
        if information['type'] == 'B':
            if any(code == str(information['price']) for code in bid.columns.values.tolist()):
                bid[str(information['price'])] += int(float(information['quantity']))
            else:
                bid[str(information['price'])] = int(float(information['quantity']))
        else:
            if any(code == str(information['price']) for code in ask.columns.values.tolist()):
                ask[str(information['price'])] += int(float(information['quantity']))
            else:
                ask[str(information['price'])] = int(float(information['quantity']))
        order_book[str(information['id'])] = [int(float(information['quantity'])), information['price'],
                                              information['type']]
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been added at price " + str(order_book[str(information['id'])][1]))
        type = 'Order Added'

    elif message.EventCode == 'E':
        information = order_excuted_handler(message)
        #    print(information['time'] + ": " + str(information['quantity'])
        #        + " shares of order just executed")
        order_to_delete = str(information['id'])
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        order_price = str(order_book[order_to_delete][1])
        position = 0
        for order_price_iterator in bid.columns.values.tolist():
            if order_price_iterator == order_price:
                bid.iloc[0, position] -= int(float(information['quantity']))
                if bid.iloc[0, position] <= 0:
                    bid.drop(bid.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        for order_price_iterator in ask.columns.values.tolist():
            if order_price_iterator == order_price:
                ask.iloc[0, position] -= int(float(information['quantity']))
                if ask.iloc[0, position] <= 0:
                    ask.drop(ask.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just executed at price " + str(order_book[str(information['id'])][1]))
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)
        type = 'Order Executed'

    elif message.EventCode == 'C':
        information = price_order_excuted_handler(message)
        #    print(information['time'] + ": " + str(information['quantity']) +
        #          " shares of order just executed as price of " + str(information['price']))
        order_to_delete = str(information['id'])
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        order_price = order_book[order_to_delete][1]
        position = 0
        for order_price_iterator in bid.columns.values.tolist():
            if order_price_iterator == order_price:
                bid.iloc[0, position] -= int(information['quantity'])
                if bid.iloc[0, position] <= 0:
                    bid.drop(bid.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        for order_price_iterator in ask.columns.values.tolist():
            if order_price_iterator == order_price:
                ask.iloc[0, position] -= int(information['quantity'])
                if ask.iloc[0, position] <= 0:
                    ask.drop(ask.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just executed at price " + str(order_book[str(information['id'])][1]))
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)
        type = 'Order Executed'

    elif message.EventCode == 'U':
        information = replace_order_handler(message)
        #    print(str(information['time'])+ ": " + str(information['quantity']) +
        #          " shares of order has been replaced")
        old_quantity = int(float(order_book[str(information['id'])][0]))
        old_price = str(order_book[str(information['id'])][1])
        old_type = order_book[str(information['id'])][2]
        order_book[str(information['newid'])] = [int(float(information['quantity'])), information['price'], old_type]
        old_order_id = str(information['id'])
        new_order_id = str(information['newid'])
        new_price = str(information['price'])
        new_quantity = int(float(information['quantity']))
        position = 0
        for order_price_iterator in bid.columns.values.tolist():
            if order_price_iterator == old_price:
                bid.iloc[0, position] -= old_quantity
                if bid.iloc[0, position] <= 0:
                    bid.drop(bid.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        for order_price_iterator in ask.columns.values.tolist():
            if order_price_iterator == old_price:
                ask.iloc[0, position] -= old_quantity
                if ask.iloc[0, position] <= 0:
                    ask.drop(ask.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        if old_type == 'B':
            if any(code == str(new_price) for code in bid.columns.values.tolist()):
                bid[new_price] += new_quantity
            else:
                bid[new_price] = new_quantity
        else:
            if any(code == str(new_price) for code in ask.columns.values.tolist()):
                ask[new_price] += new_quantity
            else:
                ask[new_price] = new_quantity
        fd.write(str(information['time']) + ": " + str(old_quantity) +
                 " shares of order at price " + str(old_price) +
                 " has been replaced as " + str(new_quantity) + " shares of order at price " +
                 str(new_price))
        order_book = order_book.drop(str(information['id']), 1)
        type = 'Order Replaced'


    elif message.EventCode == 'D':
        information = delete_order_handler(message)
        print(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
              " shares of order has been deleted at price " + str(order_book[str(information['id'])][1]))
        order_to_delete = str(information['id'])
        delete_quantity = order_book[order_to_delete][0]
        order_price = str(order_book[order_to_delete][1])
        position = 0
        for order_price_iterator in bid.columns.values.tolist():
            if order_price_iterator == order_price:
                bid.iloc[0, position] -= delete_quantity
                if bid.iloc[0, position] <= 0:
                    bid.drop(bid.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        for order_price_iterator in ask.columns.values.tolist():
            if order_price_iterator == order_price:
                ask.iloc[0, position] -= delete_quantity
                if ask.iloc[0, position] <= 0:
                    ask.drop(ask.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        fd.write(str(information['time']) + ":" + str(order_book[str(information['id'])][0]) +
                 " shares of order has been deleted at price " + str(order_book[str(information['id'])][1]))
        order_book = order_book.drop(order_to_delete, 1)
        type = 'Order Deleted'

    elif message.EventCode == 'X':
        information = canel_order_handler(message)
        #    print(information['time'] + ": " + str(information['quantity'])
        #        + " shares of order just executed")
        order_to_delete = str(information['id'])
        order_book[order_to_delete][0] -= int(float(information['quantity']))
        order_price = str(order_book[order_to_delete][1])
        position = 0
        for order_price_iterator in bid.columns.values.tolist():
            if order_price_iterator == order_price:
                bid.iloc[0, position] -= int(float(information['quantity']))
                if bid.iloc[0, position] <= 0:
                    bid.drop(bid.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        position = 0
        for order_price_iterator in ask.columns.values.tolist():
            if order_price_iterator == order_price:
                ask.iloc[0, position] -= int(float(information['quantity']))
                if ask.iloc[0, position] <= 0:
                    ask.drop(ask.columns.values[position], axis=1, inplace=True)
                    break
            position += 1
        fd.write(information['time'] + ": " + str(information['quantity']) +
                 " shares of order just canceled  at price " + str(order_book[str(information['id'])][1]))
        if order_book[order_to_delete][0] <= 0:
            order_book = order_book.drop(order_to_delete, 1)
        type = 'Order Canceled'

    else:
        print('Wrong Message Type!')

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
