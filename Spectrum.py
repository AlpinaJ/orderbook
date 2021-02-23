from math import *

class Spectrum(object):

    def __init__(self, orderbook):
        self.orderbook = orderbook
        if self.orderbook.seccode in ["EUR_RUB__TOD", "EUR_RUB__TOM", "USD000000TOD", "USD000UTSTOM"]:
            self.price_step = 0.0025
        else:
            self.price_step = 0.0001
        self.depth = 50

    def calculate_spectrum(self, side):

        normalized_spectrum = []

        if (side == 'B' and self.orderbook.bids.keys().__len__() != 0) or (side == 'S' and self.orderbook.asks.keys().__len__() != 0):

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
            total_volume = 0

            for i in range(self.depth):
                volume = 0
                if current_price in orders:
                    for ordernumber in orders[current_price]:
                        volume += self.orderbook.orders[ordernumber]
                spectrum.append(volume)
                total_volume += volume
                current_price += direction * self.price_step
                current_price = int((current_price + 0.00003) * 10000) / 10000

            five_step_volume = 0
            for i in range(len(spectrum)):
                five_step_volume += spectrum[i]
                if i % 5 == 4:
                    normalized_spectrum.append(five_step_volume / total_volume)
                    five_step_volume = 0

        return normalized_spectrum

    def avg_by_count(self, spectra):
        avg_by_count_spectrum = []
        for i in range(self.depth//5):
            sum = 0
            for j in range(1, len(spectra)):
                if len(spectra[j][1]) != 0:
                    sum += spectra[j][1][i]
            avg = sum / (len(spectra) - 1)
            avg_by_count_spectrum.append(avg)
        return avg_by_count_spectrum

    def avg_by_time(self, spectra):
        initial_ts = spectra[1][0]
        avg_by_time_spectrum = []
        for i in range(self.depth//5):
            weighted_sum = 0
            for j in range(1, len(spectra) - 1):
                if len(spectra[j][1]) != 0:
                    weighted_sum += spectra[j][1][i] * (spectra[j+1][0] - spectra[j][0])
            weighted_sum += spectra[len(spectra) - 1][1][i] * (235000000000 - spectra[len(spectra) - 1][0])
            avg = weighted_sum / (235000000000 - initial_ts)
            avg_by_time_spectrum.append(avg)
        return avg_by_time_spectrum

    def cdf(self, pdf):
        cdf = []
        cdf.append(pdf[0])
        for i in range(1, len(pdf)):
            cdf.append(cdf[i-1] + pdf[i])
        return cdf

    def ks_test(self, cdf1, cdf2, confidence_level):

        uniform_dist = abs(cdf1[0] - cdf2[0])
        for i in range(1, len(cdf1)):
            current_dist = abs(cdf1[i] - cdf2[i])
            if current_dist > uniform_dist:
                uniform_dist = current_dist

        statistical_threshold = sqrt(- 0.5 * log((1 - confidence_level) / 2))

        if sqrt(len(cdf1)) * uniform_dist > statistical_threshold:
            return False  # distributions are different
        else:
            return True  # distributions are the same

    def task3(self, bid_spectra, ask_spectra, file):

        avg_by_count_bid_spectrum = self.avg_by_count(bid_spectra)
        avg_by_count_bid_cdf = self.cdf(avg_by_count_bid_spectrum)

        avg_by_count_ask_spectrum = self.avg_by_count(ask_spectra)
        avg_by_count_ask_cdf = self.cdf(avg_by_count_ask_spectrum)

        avg_by_time_bid_spectrum = self.avg_by_time(bid_spectra)
        avg_by_time_bid_cdf = self.cdf(avg_by_time_bid_spectrum)

        avg_by_time_ask_spectrum = self.avg_by_time(ask_spectra)
        avg_by_time_ask_cdf = self.cdf(avg_by_time_ask_spectrum)

        confidence_level = 0.975

        string = ""
        string += "avg by count bid cdf: " + str(avg_by_count_bid_cdf) + "\n"
        string += "avg by time bid cdf: " + str(avg_by_time_bid_cdf) + "\n"
        string += "avg by count ask cdf: " + str(avg_by_count_ask_cdf) + "\n"
        string += "avg by time ask cdf: " + str(avg_by_time_ask_cdf) + "\n"

        string += "avg by count: bid distribution vs ask distribution: "
        string += str(self.ks_test(avg_by_count_bid_cdf, avg_by_count_ask_cdf, confidence_level))
        string += "\n"

        string += "avg by time: bid distribution vs ask distribution: "
        string += str(self.ks_test(avg_by_time_bid_cdf, avg_by_time_ask_cdf, confidence_level))
        string += "\n"

        string += "bid distributions: avg by count vs avg by time: "
        string += str(self.ks_test(avg_by_count_bid_cdf, avg_by_time_bid_cdf, confidence_level))
        string += "\n"

        string += "ask distributions: avg by count vs avg by time: "
        string += str(self.ks_test(avg_by_count_ask_cdf, avg_by_time_ask_cdf, confidence_level))
        string += "\n\n"

        file.write(string)
