import pandas as pd
from OrderBook import OrderBook
from Order import Order
from Trade import Trade


class Reader(object):
    def __init__(self, date, seccode):
        self.date = date
        self.seccode = seccode
        self.orderbook = OrderBook(seccode)
        self.matched_tradenos = []
        self.collisions = 0
        pass

    def read_file(self):
        orderlog = pd.read_csv("input/OrderLog" + self.date + ".txt", sep=',')
        current_no = 0
        while current_no <= len(orderlog) - 1:
            print(current_no)
            current_no = self.read_timestamp(current_no)

        file = open("output" + "/" + self.date + self.seccode + ".txt", "w")
        file.write("Number of collisions is ")
        file.write(str(self.collisions))
        file.write("\n")
        for key, value in self.orderbook.orders.items():
            file.write(str(key))
            file.write(":")
            file.write(str(value))
            file.write("\n")
        file.close()

    def read_timestamp(self, current_no):
        orderlog = pd.read_csv("input/OrderLog" + self.date + ".txt", sep=',')
        current_time = orderlog.loc[current_no][3]
        self.collisions = self.collisions + self.orderbook.collision()
        while (current_no <= len(orderlog) - 1) and (orderlog.loc[current_no][3] == current_time):
            orderseccode = orderlog.loc[current_no][1]
            if orderseccode == self.orderbook.seccode:
                self.execute_action(current_no)
            current_no += 1

        return current_no

    def execute_action(self, current_no):

        orderlog = pd.read_csv("input/OrderLog" + self.date + ".txt", sep=',')
        no = orderlog.loc[current_no][0]
        orderseccode = orderlog.loc[current_no][1]
        buysell = orderlog.loc[current_no][2]
        time = orderlog.loc[current_no][3]
        orderno = orderlog.loc[current_no][4]
        action = orderlog.loc[current_no][5]
        price = orderlog.loc[current_no][6]
        volume = orderlog.loc[current_no][7]
        tradeno = orderlog.loc[current_no][8]
        tradeprice = orderlog.loc[current_no][9]

        # post
        if action == 1:
            order = Order(no, orderseccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice)
            self.orderbook.post(order)

        # revoke
        elif action == 0:
            self.orderbook.revoke(orderno, buysell)

        # match
        elif action == 2 and (tradeno not in self.matched_tradenos):
            tradelog = pd.read_csv("input/TradeLog" + self.date + ".txt", sep=',')
            i = 0
            tradeno_search = tradelog.loc[i][0]
            while tradeno_search != tradeno:
                i += 1
                tradeno_search = tradelog.loc[i][0]
            buyorderno = tradelog.loc[i][3]
            sellorderno = tradelog.loc[i][4]
            trade = Trade(tradeno, orderseccode, time, buyorderno, sellorderno, price, volume)
            self.orderbook.match(trade)
            self.matched_tradenos.append(tradeno)




if __name__ == '__main__':
    reader = Reader("20180301", "EUR_RUB__TOD")
    reader.read_file()
