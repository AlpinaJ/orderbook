class Trade(object):
    def __init__(self, tradeno, seccode, time, buyorderno, sellorderno, price, volume):
        self.tradeno = tradeno
        self.seccode = seccode
        self.time = time
        self.buyorderno = buyorderno
        self.sellorderno = sellorderno
        self.price = price
        self.volume = volume
