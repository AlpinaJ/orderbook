from Order import Order
from collections import defaultdict
from Trade import Trade


class OrderBook(object):
    def __init__(self, seccode):
        self.seccode = seccode
        # map price:list of order numbers
        self.asks = defaultdict(list)
        self.bids = defaultdict(list)
        # need for decreasing volume in matches
        self.orders = defaultdict(int)
        self.collisions = 0
        self.matches = 0
        self.current_timestamp = 0

    def revoke(self, orderno, buysell) -> None:
        if orderno in self.orders:
            self.orders.pop(orderno)
        else:
            self.collisions += 1

        flag = 0
        if buysell == 'S':
            for key, value in self.asks.items():
                if value is not None:
                    for i in value:
                        if i == orderno:
                            if len(self.asks[key]) < 2:
                                self.asks.pop(key)
                                flag = 1
                            else:
                                value.remove(i)
                                self.asks[key] = value
                                flag = 1
                    if flag == 1:
                        break

        if buysell == 'B':
            for key, value in self.bids.items():
                if value is not None:
                    for i in value:
                        if i == orderno:
                            if len(self.bids[key]) < 2:
                                self.bids.pop(key)
                                flag = 1
                            else:
                                value.remove(i)
                                flag = 1
                    if flag == 1:
                        break

    def post(self, order: Order) -> None:
        self.orders.setdefault(order.orderno, order.volume)
        if order.buysell == 'B':
            self.bids[order.price].append(order.orderno)
        if order.buysell == 'S':
            self.asks[order.price].append(order.orderno)

    def decrease_order_volume(self, orderno, delta_volume, buysell):
        self.orders[orderno] = self.orders[orderno] - delta_volume
        if self.orders[orderno] <= 0:
            self.revoke(orderno, buysell)
        # if self.orders[orderno] <0:
        #     self.collisions += 1

    def match(self, trade: Trade) -> None:
        buyer = trade.buyorderno
        seller = trade.sellorderno
        trade_volume = trade.volume
        self.matches += 1

        self.decrease_order_volume(buyer, trade_volume, 'B')
        self.decrease_order_volume(seller, trade_volume, 'S')

    def collision(self) -> int:
        if len(self.asks.keys()) > 0 and len(self.bids.keys()) > 0 and min(self.asks.keys()) < max(self.bids.keys()):
            return 1
        else:
            return 0

    def task1(self, file):

        asks_keys = sorted(self.asks.keys())
        bids_keys = sorted(self.bids.keys())

        string = "Number of collisions is " + str(self.collisions - self.matches) + "\nAsks\n"
        file.write(string)

        for price in asks_keys:
            total_volume = 0
            if self.asks[price] is not None:
                for ordernumber in self.asks[price]:
                    total_volume += self.orders[ordernumber]
                s = str(price) + " : " + str(total_volume) + "\n"
                file.write(s)

        file.write("\nBids\n")
        for price in bids_keys:
            total_volume = 0
            if self.bids[price] is not None:
                for ordernumber in self.bids[price]:
                    total_volume += self.orders[ordernumber]
                s = str(price) + " : " + str(total_volume) + "\n"
                file.write(s)
