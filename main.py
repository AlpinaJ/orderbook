import enum
import queue
import time
from Reader import Reader
from collections import defaultdict


if __name__ == '__main__':

    date = "201803"
    for i in range(1, 31):
        zero = ""
        if i < 10:
            zero = "0"
        date_i = date + zero + str(i)
        r = Reader(date_i, "USD000000TOD")
        ord = r.orderbook
        r = Reader(date_i, "USD000UTSTOM")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOD")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOM")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOD")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOM")
        ord = r.orderbook

    date = "201804"
    for i in range(2, 31):
        zero = ""
        if i < 10:
            zero = "0"
        date_i = date + zero + str(i)
        r = Reader(date_i, "USD000000TOD")
        ord = r.orderbook
        r = Reader(date_i, "USD000UTSTOM")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOD")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOM")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOD")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOM")
        ord = r.orderbook

    date = "201805"
    for i in range(2, 32):
        zero = ""
        if i < 10:
            zero = "0"
        date_i = date + zero + str(i)
        r = Reader(date_i, "USD000000TOD")
        ord = r.orderbook
        r = Reader(date_i, "USD000UTSTOM")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOD")
        ord = r.orderbook
        r = Reader(date_i, "EUR_RUB__TOM")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOD")
        ord = r.orderbook
        r = Reader(date_i, "EURUSD000TOM")
        ord = r.orderbook