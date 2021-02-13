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
        # self.print_debug()

    def post(self, order: Order) -> None:
        self.print_debug()
        self.orders.setdefault(order.orderno, order.volume)
        if order.buysell == 'B':
            self.bids[order.price].append(order.orderno)
        if order.buysell == 'S':
            self.asks[order.price].append(order.orderno)
        #self.print_debug()

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

        #self.print_debug()

    def collision(self) -> int:

        # if len(self.asks.keys()):
        #     print(min(self.asks.keys()))
        if len(self.asks.keys()) > 0 and len(self.bids.keys()) > 0 and min(self.asks.keys()) <= max(self.bids.keys()):
            return 1
        else:
            return 0


    def spectrum(self):

        price_step = 0.0025
        depth = 50
        normalized_bids_spectrum = []
        normalized_asks_spectrum = []

        if self.bids.keys().__len__() != 0:

            best_bid = max(self.bids.keys())
            current_price = best_bid
            bids_spectrum = []
            total_bids_volume = 0

            for i in range(depth):
                volume = 0
                if current_price in self.bids:
                    for ordernumber in self.bids[current_price]:
                        volume += self.orders[ordernumber]
                bids_spectrum.append(volume)
                total_bids_volume += volume
                current_price -= price_step

            five_step_volume = 0
            for i in range(len(bids_spectrum)):
                five_step_volume += bids_spectrum[i]
                if i % 5 == 4:
                    normalized_bids_spectrum.append(five_step_volume / total_bids_volume)
                    five_step_volume = 0

        if self.asks.keys().__len__() != 0:

            best_ask = min(self.asks.keys())
            current_price = best_ask
            asks_spectrum = []
            total_asks_volume = 0

            for i in range(depth):
                volume = 0
                if current_price in self.asks:
                    for ordernumber in self.asks[current_price]:
                        volume += self.orders[ordernumber]
                asks_spectrum.append(volume)
                total_asks_volume += volume
                current_price += price_step

            five_step_volume = 0
            for i in range(len(asks_spectrum)):
                five_step_volume += asks_spectrum[i]
                if i % 5 == 4:
                    normalized_asks_spectrum.append(five_step_volume / total_asks_volume)
                    five_step_volume = 0

        return normalized_bids_spectrum, normalized_asks_spectrum

    def print_debug(self):
        # print("orders:", self.orders)
        # print("asks:", self.asks)
        # print("bids", self.bids)
        # print("-----------------------")
        pass


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
