class MyQueue(object):

    def __init__(self, T, side, max_band):
        self.T = T
        self.side = side
        self.queue = []
        self.max_band = max_band

    def add(self, timestamp, order):
        self.queue.append([timestamp, order])

    def update(self, timestamp):
        if self.queue.__len__() != 0:
            while self.queue[0][0] < timestamp - self.T * 1000:
                self.queue.pop(0)
                if self.queue.__len__() == 0:
                    break

    def calculate_volume(self):
        volume = 0
        for i in range(len(self.queue)):
            current_order = self.queue[i][1]
            volume += current_order.volume
        return volume / (self.max_band * self.T)

    def recalculate_volume(self, top_of_the_orderbook_price, price_step):
        volume = 0
        for i in range(len(self.queue)):
            current_order = self.queue[i][1]
            if abs(top_of_the_orderbook_price - current_order.price) < 5 * price_step + 0.00001:
                volume += current_order.volume
        return volume / (self.max_band * self.T)
