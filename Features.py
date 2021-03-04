from math import *


class Features(object):

    def __init__(self, orderbook, max_band, price_step):
        self.orderbook = orderbook
        self.price_step = price_step
        self.max_band = max_band

    def spectrum(self, side):

        normalized_spectrum = []
        depth = 50

        if (side == 'B' and self.orderbook.bids.keys().__len__() != 0) or (
                side == 'S' and self.orderbook.asks.keys().__len__() != 0):

            if side == 'B':
                orders = self.orderbook.bids
                best_order = max(orders.keys())
                direction = -1
            else:
                orders = self.orderbook.asks
                best_order = min(orders.keys())
                direction = 1

            current_price = best_order
            spectrum = []

            for i in range(depth):
                volume = 0
                if current_price in orders:
                    for ordernumber in orders[current_price]:
                        volume += self.orderbook.orders[ordernumber]
                spectrum.append(volume)
                current_price += direction * self.price_step
                current_price = int((current_price + 0.00003) * 10000) / 10000

            five_step_volume = 0
            for i in range(len(spectrum)):
                five_step_volume += spectrum[i]
                if i % 5 == 4:
                    normalized_spectrum.append(five_step_volume / self.max_band)
                    five_step_volume = 0

        return normalized_spectrum

    def vwap(self, band, side):

        if self.orderbook.bids.keys().__len__() == 0 or self.orderbook.asks.keys().__len__() == 0:
            return 0

        if side == 'B':
            orders = self.orderbook.bids
            keys = sorted(self.orderbook.bids.keys())
            keys.reverse()
        elif side == 'S':
            orders = self.orderbook.asks
            keys = sorted(self.orderbook.asks.keys())

        vwap = 0
        volume = 0
        flag = 0
        for price in keys:
            if orders[price] is not None:
                for ordernumber in orders[price]:
                    current_order_volume = self.orderbook.orders[ordernumber]
                    if volume + current_order_volume <= band:
                        vwap += price * current_order_volume
                        volume += current_order_volume
                    else:
                        vwap += price * (band - volume)
                        volume = band
                        flag = 1
                        break
            if flag == 1:
                break

        vwap /= volume

        best_bid = max(self.orderbook.bids.keys())
        best_ask = min(self.orderbook.asks.keys())
        midpoint = best_bid + (best_ask - best_bid) / 2

        if side == 'S':
            normalized_vwap = vwap - midpoint
        else:
            normalized_vwap = midpoint - vwap

        normalized_vwap /= self.price_step

        return normalized_vwap

    def bid_ask_spread(self):
        if self.orderbook.bids.keys().__len__() != 0 and self.orderbook.asks.keys().__len__() != 0:
            best_bid = max(self.orderbook.bids.keys())
            best_ask = min(self.orderbook.asks.keys())
            return int(((best_ask - best_bid) / self.price_step) + 0.0001)
        else:
            return inf

    def print_features(self, outputpath, spectra, bid_ask_spreads, liquidity_taking, liquidity_making, vwaps):
        spectra_file = open(outputpath + "_spectrum.txt", "a")
        string = "TIMESTAMP,BID SPECTRUM,ASK SPECTRUM\n"
        spectra_file.write(string)
        for i in range(len(spectra)):
            string = ""
            string += str(spectra[i][0]) + ","
            for j in range(len(spectra[i][1])):
                string += str(spectra[i][1][j])
                if j != len(spectra[i][1]) - 1:
                    string += " "
            string += ","
            for j in range(len(spectra[i][2])):
                string += str(spectra[i][2][j]) + " "
                if j != len(spectra[i][2]) - 1:
                    string += " "
            string += "\n"
            spectra_file.write(string)

        bid_ask_spreads_file = open(outputpath + "_bid_ask_spread.txt", "a")
        string = "TIMESTAMP,BID-ASK SPREAD"
        bid_ask_spreads_file.write(string)
        for i in range(len(bid_ask_spreads)):
            string = str(bid_ask_spreads[i][0]) + "," + str(bid_ask_spreads[i][1]) + '\n'
            bid_ask_spreads_file.write(string)

        liquidity_taking_file = open(outputpath + "_liquidity_taking.txt", "a")
        string = "TIMESTAMP,SIDE,T,VOLUME\n"
        liquidity_taking_file.write(string)
        for i in range(10, len(liquidity_taking)):
            string = str(liquidity_taking[i][0]) + " "
            string += str(liquidity_taking[i][1]) + " "
            string += str(liquidity_taking[i][2]) + " "
            string += str(liquidity_taking[i][3]) + "\n"
            liquidity_taking_file.write(string)

        liquidity_making_file = open(outputpath + "_liquidity_making.txt", "a")
        string = "TIMESTAMP,SIDE,T,VOLUME\n"
        liquidity_making_file.write(string)
        for i in range(10, len(liquidity_making)):
            string = str(liquidity_making[i][0]) + " "
            string += str(liquidity_making[i][1]) + " "
            string += str(liquidity_making[i][2]) + " "
            string += str(liquidity_making[i][3]) + "\n"
            liquidity_making_file.write(string)

        vwaps_file = open(outputpath + "_vwaps.txt", "a")
        string = "TIMESTAMP,SIDE,BAND,VWAP\n"
        vwaps_file.write(string)
        for i in range(12, len(vwaps)):
            string = str(vwaps[i][0]) + " "
            string += str(vwaps[i][1]) + " "
            string += str(vwaps[i][2]) + " "
            string += str(vwaps[i][3]) + "\n"
            vwaps_file.write(string)
