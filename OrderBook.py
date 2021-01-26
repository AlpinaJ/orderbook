from Order import  Order
from collections import defaultdict
from Trade import Trade


class OrderBook(object):
    def __init__(self, seccode):
        self.seccode = seccode
        # map price:list of order numbers
        self.asks = defaultdict(list)
        self.bids = defaultdict(list)
        # need for decreasing volume in mathces
        self.orderds = defaultdict(int)

    def revoke(self, orderno, buysell) -> None:
        self.orderds.pop(orderno)
        flag = 0
        if buysell == 'S':
            for key, value in self.asks.items():
                for i in value:
                    if i is orderno:
                        if len(self.asks[key]) < 2:
                            self.asks.pop(key)
                            flag = 1
                        else:
                            new_value = value.remove(i)
                            self.asks[key] = new_value
                            flag = 1
                if flag == 1:
                    break

        if buysell == 'B':
            for key, value in self.bids.items():
                for i in value:
                    if i is orderno:
                        if len(self.bids[key]) < 2:
                            self.bids.pop(key)
                            flag = 1
                        else:
                            new_value = value.remove(i)
                            self.bids[key] = new_value
                            flag = 1
                if flag == 1:
                    break

    def post(self, order: Order) -> None:
        self.orderds.setdefault(order.orderno, order.volume)
        if order.buysell == 'B':
            self.bids[order.price].append(order.orderno)
        else:
            self.asks[order.price].append(order.orderno)

    def decrease_order_volume(self, orderno, delta_volume, buysell):
        self.orderds[orderno] = self.orderds[orderno] - delta_volume
        if self.orderds[orderno] == 0:
            self.revoke(orderno, buysell)

    def match(self, trade: Trade) -> None:
        buyer = trade.buyorderno
        seller = trade.sellorderno
        trade_volume = trade.volume

        self.decrease_order_volume(buyer, trade_volume, 'B')
        self.decrease_order_volume(seller, trade_volume, 'S')


    def print_debug(self):
        print("orders:", self.orderds)
        print("asks:", self.asks)
        print("bids", self.bids)
        print("-----------------------")



if __name__ == '__main__':
    a = Order(1, 'usdrub', 'B', 1, 1, 1, 67, 2, 0, 0)
    c = Order(2, 'usdrub', 'S', 1, 2, 1, 65, 4, 0, 0)
    d = Order(3, 'usdrub', 'B', 1, 3, 1, 66, 1, 0, 0)

    ac = Trade(1, 'usdrub', 10, 1, 2, 65, 2)
    cd = Trade(1, 'usdrub', 11, 3, 2, 65, 1)

    Book = OrderBook('usdrub')

    Book.post(a)
    Book.print_debug()

    Book.post(c)
    Book.print_debug()

    Book.post(d)
    Book.print_debug()

    Book.match(ac)
    Book.print_debug()

    Book.match(cd)
    Book.print_debug()
    # Book.revoke(2, 'S')
    # Book.print_debug()
    #
    # Book.revoke(1, 'B')
    # Book.print_debug()


