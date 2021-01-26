class Order(object):
    def __init__(self, no, seccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice):
        self.side = no
        self.seccode = seccode
        self.buysell = buysell
        self.time = time
        self.orderno = orderno
        self.action = action
        self.price = price
        self.volume = volume
        self.tradeno = tradeno
        self.tradeprice = tradeprice