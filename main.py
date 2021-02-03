import enum
import queue
import time
from Reader import Reader
from collections import defaultdict

if __name__ == '__main__':
    date = "201803"
    # for i in [1,2,5,6,7,9,12,13,14,15,16,19,20,21,22,23,26,27,28,29,30]:
    for i in [1]:
        zero = ""
        if i < 10:
            zero = "0"
        date_i = date + zero + str(i)
        r = Reader(date_i, "USD000000TOD")
        r.read_file()
        ord = r.orderbook
        r = Reader(date_i, "USD000UTSTOM")
        r.read_file()
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOD")
        r.read_file()
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOM")
        r.read_file()
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOD")
        r.read_file()
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOM")
        r.read_file()
        ord = r.orderbook

    # date = "201804"
    # for i in [2,3,4,5,6,9,10,11,12,13,16,17,18,19,20,23,24,25,26,27,28,30]:
    #     zero = ""
    #     if i < 10:
    #         zero = "0"
    #     date_i = date + zero + str(i)
    #     r = Reader(date_i, "USD000000TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "USD000UTSTOM")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EUR_RUB__TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EUR_RUB__TOM")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EURUSD000TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EURUSD000TOM")
    #     ord = r.orderbook
    #
    # date = "201805"
    # for i in [2,3,4,7,8,10,11,14,15,16,17,18,21,22,23,24,25,28,29,30,31]:
    #     zero = ""
    #     if i < 10:
    #         zero = "0"
    #     date_i = date + zero + str(i)
    #     r = Reader(date_i, "USD000000TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "USD000UTSTOM")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EUR_RUB__TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EUR_RUB__TOM")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EURUSD000TOD")
    #     ord = r.orderbook
    #     r = Reader(date_i, "EURUSD000TOM")
    #     ord = r.orderbook
