import pandas as pd
from OrderBook import OrderBook
from Order import Order
from collections import defaultdict
from Trade import Trade
from Spectrum import Spectrum
from Features import Features
from MyQueue import MyQueue
import math

BATCH_SIZE = 100000


def read_batch(path, batch_size):
    print(f"Start reading {path}")
    for chunk in pd.read_csv(path, chunksize=batch_size):
        for row in chunk.itertuples():
            order = Order(no=row.NO,
                          seccode=row.SECCODE,
                          buysell=row.BUYSELL,
                          time=row.TIME,
                          orderno=row.ORDERNO,
                          action=row.ACTION,
                          price=row.PRICE,
                          volume=row.VOLUME,
                          tradeno=row.TRADENO,
                          tradeprice=row.TRADEPRICE)
            if order.side % 1000000 == 0:
                print(f"Processed {order.side} orders...")
            yield order

    print(f"No more rows in {path}")

"""
def process_single_orderlog_task4(path, seccode, tradepath, outputpath, day):
    orderbook = OrderBook(seccode=seccode)
    spectrum = Spectrum(orderbook)
    bid_spectra = []
    ask_spectra = []
    bid_spectra1 = []
    ask_spectra1 = []
    bid_spectra2 = []
    ask_spectra2 = []
    bid_spectra3 = []
    ask_spectra3 = []
    trades = defaultdict(Trade)
    trades = process_tradelogs(tradepath)
    processed_trades = []
    file = open(outputpath, "a")
    # file.write(seccode + "\n")
    flag1 = 0
    flag2 = 0

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):

        if order.seccode != seccode:
            continue

        if orderbook.current_timestamp < order.time:

            if orderbook.current_timestamp >= 150000000000 and flag1 == 0:
                flag1 = 1

            if orderbook.current_timestamp >= 190000000000 and flag2 == 0:
                flag2 = 1

            orderbook.collisions += orderbook.collision()

            current_bid_spectrum = spectrum.calculate_spectrum('B')
            # bid_spectra.append([orderbook.current_timestamp, current_bid_spectrum])
            current_ask_spectrum = spectrum.calculate_spectrum('S')
            # ask_spectra.append([orderbook.current_timestamp, current_ask_spectrum])

            if (flag1 == 0):
                bid_spectra1.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra1.append([orderbook.current_timestamp, current_ask_spectrum])

            if (flag2 == 0):
                bid_spectra2.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra2.append([orderbook.current_timestamp, current_ask_spectrum])

            if (flag2 == 1 and flag1 == 1):
                bid_spectra3.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra3.append([orderbook.current_timestamp, current_ask_spectrum])

            string = str(orderbook.current_timestamp) + '\n'
            string += "Bids spectrum:"
            for i in range(len(current_bid_spectrum)):
                string += " " + str(current_bid_spectrum[i])
            string += '\n'
            string += "Asks spectrum:"
            for i in range(len(current_ask_spectrum)):
                string += " " + str(current_ask_spectrum[i])
            string += '\n'
            file.write(string)



            if order.time >= 235000000000:
                break

            orderbook.current_timestamp = order.time

        if order.action == 0:
            orderbook.revoke(order.orderno, order.buysell)
        elif order.action == 1:
            orderbook.post(order)
        elif order.action == 2:
            if order.tradeno in processed_trades:
                continue
            else:
                orderbook.match(trades[order.tradeno])
                processed_trades.append(order.tradeno)
                orderbook.collisions -= 1

    spectrum.task4(bid_spectra1, ask_spectra1, bid_spectra2, ask_spectra2, bid_spectra3, ask_spectra3, file, day)
    file.close()
    print(f"Finished with reading {path}")


def process_single_orderlog_task42(path, seccode, tradepath):
    orderbook = OrderBook(seccode=seccode)
    spectrum = Spectrum(orderbook)
    bid_spectra1 = []
    ask_spectra1 = []
    bid_spectra2 = []
    ask_spectra2 = []
    bid_spectra3 = []
    ask_spectra3 = []
    trades = defaultdict(Trade)
    trades = process_tradelogs(tradepath)
    processed_trades = []
    flag1 = 0
    flag2 = 0

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):

        if order.seccode != seccode:
            continue

        if orderbook.current_timestamp < order.time:

            if orderbook.current_timestamp >= 150000000000 and flag1 == 0:
                flag1 = 1

            if orderbook.current_timestamp >= 190000000000 and flag2 == 0:
                flag2 = 1

            orderbook.collisions += orderbook.collision()

            current_bid_spectrum = spectrum.calculate_spectrum('B')
            current_ask_spectrum = spectrum.calculate_spectrum('S')

            if (flag1 == 0):
                bid_spectra1.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra1.append([orderbook.current_timestamp, current_ask_spectrum])

            if (flag2 == 0):
                bid_spectra2.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra2.append([orderbook.current_timestamp, current_ask_spectrum])

            if (flag2 == 1 and flag1 == 1):
                bid_spectra3.append([orderbook.current_timestamp, current_bid_spectrum])
                ask_spectra3.append([orderbook.current_timestamp, current_ask_spectrum])

            if order.time >= 235000000000:
                break

            orderbook.current_timestamp = order.time

        if order.action == 0:
            orderbook.revoke(order.orderno, order.buysell)
        elif order.action == 1:
            orderbook.post(order)
        elif order.action == 2:
            if order.tradeno in processed_trades:
                continue
            else:
                orderbook.match(trades[order.tradeno])
                processed_trades.append(order.tradeno)
                orderbook.collisions -= 1
    print(f"Finished with reading {path}")
    ask_arr = [ask_spectra1, ask_spectra2, ask_spectra3]
    bid_arr = [bid_spectra1, bid_spectra2, bid_spectra3]
    return ask_arr, bid_arr


def process_single_orderlog_task5(path, seccode, tradepath, outputpath):
    orderbook = OrderBook(seccode=seccode)
    trades = process_tradelogs(tradepath)
    processed_trades = []

    price_step = 0.0025
    max_band = 10000000
    features = Features(orderbook, max_band, price_step)

    spectra = []
    bid_ask_spreads = []
    liquidity_taking = []
    liquidity_making = []
    vwaps = []

    Ts = [1, 5, 15, 30, 60]

    liquidity_taking_queues = []
    liquidity_making_queues = []
    for T in Ts:
        liquidity_taking_queues.append(MyQueue(T, 'B', max_band))
        liquidity_taking_queues.append(MyQueue(T, 'S', max_band))
        liquidity_making_queues.append(MyQueue(T, 'B', max_band))
        liquidity_making_queues.append(MyQueue(T, 'S', max_band))

    bands = [100000, 200000, 500000, 1000000, 5000000, 10000000]

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):

        if order.seccode != seccode:
            continue

        if orderbook.current_timestamp < order.time:

            spectra.append([orderbook.current_timestamp, features.spectrum('B'), features.spectrum('S')])
            bid_ask_spreads.append([orderbook.current_timestamp, features.bid_ask_spread()])

            for q in liquidity_taking_queues:
                q.update(orderbook.current_timestamp)
                volume = q.calculate_volume()
                liquidity_taking.append([orderbook.current_timestamp, q.side, q.T, volume])

            for q in liquidity_making_queues:
                q.update(orderbook.current_timestamp)
                if q.side == 'B':
                    if orderbook.bids.keys().__len__() == 0:
                        volume = 0
                    else:
                        best_bid = max(orderbook.bids.keys())
                        volume = q.recalculate_volume(best_bid, price_step)
                if q.side == 'S':
                    if orderbook.asks.keys().__len__() == 0:
                        volume = 0
                    else:
                        best_ask = min(orderbook.asks.keys())
                        volume = q.recalculate_volume(best_ask, price_step)
                liquidity_making.append([orderbook.current_timestamp, q.side, q.T, volume])

            for band in bands:
                vwaps.append([orderbook.current_timestamp, 'B', band, features.vwap(band, 'B')])
                vwaps.append([orderbook.current_timestamp, 'S', band, features.vwap(band, 'S')])

            if order.time >= 235000000000:
                break

            orderbook.current_timestamp = order.time

        if order.action == 0:
            orderbook.revoke(order.orderno, order.buysell)

        elif order.action == 1:
            orderbook.post(order)

            # passive buyer ~ does not cross the bid-ask spread
            if order.buysell == 'B' and (
                    orderbook.asks.keys().__len__() == 0 or order.price < min(orderbook.asks.keys())):
                for q in liquidity_making_queues:
                    if q.side == 'B':
                        q.add(orderbook.current_timestamp, order)

            # passive seller ~ does not cross the bid-ask spread
            if order.buysell == 'S' and (
                    orderbook.bids.keys().__len__() == 0 or order.price > max(orderbook.bids.keys())):
                for q in liquidity_making_queues:
                    if q.side == 'S':
                        q.add(orderbook.current_timestamp, order)

        elif order.action == 2:
            if order.tradeno in processed_trades:
                continue
            else:
                orderbook.match(trades[order.tradeno])
                processed_trades.append(order.tradeno)

                # aggressive buyer
                if trades[order.tradeno].buyorderno > trades[order.tradeno].sellorderno:
                    for q in liquidity_taking_queues:
                        if q.side == 'B':
                            q.add(orderbook.current_timestamp, order)

                # aggressive seller
                if trades[order.tradeno].buyorderno < trades[order.tradeno].sellorderno:
                    for q in liquidity_taking_queues:
                        if q.side == 'S':
                            q.add(orderbook.current_timestamp, order)

    features.print_features(outputpath, spectra, bid_ask_spreads, liquidity_taking, liquidity_making, vwaps)
    print(f"Finished with reading {path}")

"""

def process_tradelogs(path):
    trades = defaultdict(Trade)
    for chunk in pd.read_csv(path, chunksize=BATCH_SIZE):
        for row in chunk.itertuples():
            trade = Trade(tradeno=row.TRADENO, seccode=row.SECCODE, time=row.TIME,
                          buyorderno=row.BUYORDERNO, sellorderno=row.SELLORDERNO,
                          price=row.PRICE, volume=row.VOLUME)
            trades.setdefault(trade.tradeno, trade)

    return trades


def process_single_orderlog_task7(path, tradepath, seccode):
    orderbook = OrderBook(seccode=seccode)
    trades = defaultdict(Trade)
    trades = process_tradelogs(tradepath)
    processed_trades = []
    midpoints = []
    timestamps = []
    last_midpoint = 0

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):

        if order.seccode != seccode:
            continue
        if orderbook.current_timestamp < order.time:
            if order.time >= 235000000000:
                break
            flag = 1
            if not (len(orderbook.asks.keys()) > 0 and len(orderbook.bids.keys()) > 0):
                flag = 0
            if flag == 1:
                current_midpoint = (min(orderbook.asks.keys()) - max(orderbook.bids.keys())) / 2
                if (current_midpoint != last_midpoint and current_midpoint > 0):
                    midpoints.append(current_midpoint)
                    last_midpoint = current_midpoint
                    timestamps.append(orderbook.current_timestamp)
                # print(midpoints)
            orderbook.current_timestamp = order.time

        if order.action == 0:
            orderbook.revoke(order.orderno, order.buysell)
        elif order.action == 1:
            orderbook.post(order)
        elif order.action == 2:
            if order.tradeno in processed_trades:
                continue
            else:
                orderbook.match(trades[order.tradeno])
                processed_trades.append(order.tradeno)

    print(f"Finished with reading {path}")

    return midpoints, timestamps


def var(array):
    result = 0
    for a in array:
        result += a * a
    return math.sqrt(result)


def overlap(x1, x2, y1, y2):
    xmax = max(x1, x2)
    xmin = min(x1, x2)
    ymax = max(y1, y2)
    ymin = min(y1, y2)
    # b-----a
    a = min(xmax, ymax)
    b = max(xmin, ymin)
    if (a - b) > 0:
        return 1
    else:
        return 0


def corr(varx, vary, x, y, timestampsx, timestampsy, lenx, leny):
    result = 0
    count = 0
    last_checked_timestamp = 1
    flag = 0
    for i in range(1, lenx):
        for j in range(last_checked_timestamp, leny):
            if overlap(timestampsx[i], timestampsx[i - 1], timestampsy[j], timestampsy[j - 1]) == 1:
                result = result + (x[i] - x[i - 1]) * (y[j] - y[j - 1])
                count += 1
                last_checked_timestamp = j
                flag = 1
            else:
                if flag == 1:
                    flag = 0
                    break

    result = result / vary / varx / count
    return result


if __name__ == '__main__':

    orderlog = "input/OrderLog20180301test.txt"
    tradelog = "input/TradeLog20180301.txt"
    seccode1 = "USD000UTSTOM"
    seccode2 = "EUR_RUB__TOM"
    midpoints1, timestamps1 = process_single_orderlog_task7(orderlog, tradelog, seccode1)
    midpoints2, timestamps2 = process_single_orderlog_task7(orderlog, tradelog, seccode2)
    file = open("task7.txt", "a")

    mean = 0
    k = 0
    # print(midpoints1, " ", midpoints2, "\n")
    for m in midpoints1:
        mean = mean + m
        k = k + 1
    mean = mean / k
    i = 0
    while i < k:
        midpoints1[i] = midpoints1[i] - mean
        i = i + 1
    len1 = k

    mean = 0
    k = 0
    for m in midpoints2:
        mean = mean + m
        k = k + 1
    mean = mean / k
    i = 0
    while i < k:
        midpoints2[i] = midpoints2[i] - mean
        i = i + 1
    len2 = k
    # print(len1," ",len2, "\n")
    #print(midpoints1, " ", midpoints2, "\n")
    # print(timestamps1, " ", timestamps2, "\n")

    max_corr = 0
    max_corr_tau = -5000
    var1 = var(midpoints1)
    var2 = var(midpoints2)
    for tau in range(-5000, 5001, 100):
        #if tau == 0:
        #    continue
        shifted_timestamps2 = []
        for i in range(len(timestamps2)):
            shifted_timestamps2.append(timestamps2[i] + tau)
        current_corr = corr(var1, var2, midpoints1, midpoints2, timestamps1, shifted_timestamps2, len1, len2)
        file.write(str(tau) + ": " + str(current_corr) + "\n")
        if abs(current_corr) > max_corr:
            max_corr = current_corr
            max_corr_tau = tau

    file.write("Tau: " + str(max_corr_tau) + "\n")
    if max_corr > 0:
        file.write(seccode1 + " is leading")
    else:
        file.write(seccode2 + " leading")

    # process_single_orderlog_task5(path="input/OrderLog20180301.txt", seccode="EUR_RUB__TOD", tradepath="input/TradeLog20180301.txt", outputpath="output/20180301_EUR_RUB__TOD")

    # dates = ["301", "302", "305","306", "307", "309", "312", "313", "314", "315", "316", "319", "320",
    #          "321", "322", "323", "326", "327", "328", "329", "330", "402", "403", "404", "405", "406",
    #          "409", "410", "411", "412", "413", "416", "417", "418", "420", "423", "424", "425", "426",
    #          "427", "428", "430", "502", "503", "504", "507", "508", "510", "511", "514", "515", "516",
    #          "517", "518", "521", "522", "523", "524", "525", "528", "529", "530", "531"]

    # seccodes = ["USD000000TOD", 'USD000UTSTOM', "EUR_RUB__TOD", "EUR_RUB__TOM"]
    # seccodes = ["USD000000TOD", 'USD000UTSTOM', "EUR_RUB__TOD", "EUR_RUB__TOM", "EURUSD000TOD", "EURUSD000TOM"]
    # seccodes4 = ["USD000UTSTOM", "EUR_RUB__TOM"]

    # date = "20180"
    # for i in dates:
    #     for sec in seccodes:
    #         curr_date = date+i
    #         process_single_orderlog_task5(path="input/OrderLog"+curr_date+".txt", seccode=sec,
    #                                 tradepath="input/TradeLog"+curr_date+".txt",
    #                                 outputpath="output/"+curr_date + sec)

    # for sec in seccodes4:
    #     process_single_orderlog(path="input/OrderLog20180301.txt", seccode=sec, tradepath="input/TradeLog20180301.txt",
    #                             outputpath="output/test.txt", day=1)
    """
    date = "20180"
    # TASK4.1
    day = 1
    for i in dates:
        for sec in seccodes4:
            curr_date = date + i
            process_single_orderlog_task4(path="input/OrderLog" + curr_date + ".txt",
                                    seccode=sec,
                                    tradepath="input/TradeLog" + curr_date + ".txt",
                                    outputpath="output/" + sec + "task4.txt",
                                    day=day)
        day += 1

    """
    # TASK 4.2
    # for sec in seccodes4:
    #     day = 1
    #     prev_ask_arr = []
    #     prev_bid_arr = []
    #     orderbook = OrderBook(seccode=sec)
    #     spectrum = Spectrum(orderbook)
    #
    #     for i in dates:
    #
    #
    #         curr_date = date + i
    #         ask_arr, bid_arr = process_single_orderlog_task42(path="input/OrderLog" + curr_date + ".txt",
    #                                                           seccode=sec,
    #                                                           tradepath="input/TradeLog" + curr_date + ".txt", )
    #         if day > 1:
    #             string = spectrum.task42(prev_ask_arr, prev_bid_arr, ask_arr, bid_arr, day)
    #             path = "output/task4/" + sec + ".txt"
    #             file = open(path, "a")
    #             file.write(string)
    #             file.close()
    #         prev_ask_arr = ask_arr
    #         prev_bid_arr = bid_arr
    #         day += 1

    # TASK 7
