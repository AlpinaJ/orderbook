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

    def revoke(self, orderno, buysell) -> None:
        self.orders.pop(orderno)
        flag = 0
        if buysell == 'S':
            for key, value in self.asks.items():
                for i in value:
                    if i == orderno:
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
                    if i == orderno:
                        if len(self.bids[key]) < 2:
                            self.bids.pop(key)
                            flag = 1
                        else:
                            new_value = value.remove(i)
                            self.bids[key] = new_value
                            flag = 1
                if flag == 1:
                    break
        #self.print_debug()

    def post(self, order: Order) -> None:
        self.orders.setdefault(order.orderno, order.volume)
        if order.buysell == 'B':
            self.bids[order.price].append(order.orderno)
        else:
            self.asks[order.price].append(order.orderno)
        #self.print_debug()

    def decrease_order_volume(self, orderno, delta_volume, buysell):
        self.orders[orderno] = self.orders[orderno] - delta_volume
        if self.orders[orderno] == 0:
            self.revoke(orderno, buysell)

    def match(self, trade: Trade) -> None:
        buyer = trade.buyorderno
        seller = trade.sellorderno
        trade_volume = trade.volume

        self.decrease_order_volume(buyer, trade_volume, 'B')
        self.decrease_order_volume(seller, trade_volume, 'S')
        self.print_debug()

    def collision(self) -> int:
        if len(self.asks.keys())>0 and len (self.bids.keys())>0 and min(self.asks.keys())<max(self.bids.keys()):
            print("collision")
            return 1
        else:
            return 0

    def print_debug(self):
        print("orders:", self.orders)
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
