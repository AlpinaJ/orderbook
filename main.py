import pandas as pd
from OrderBook import OrderBook
from Order import Order
from collections import defaultdict
from Trade import Trade
from Spectrum import Spectrum

BATCH_SIZE = 1000000


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


def process_single_orderlog(path, seccode, tradepath, outputpath, day):
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
            """
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
            """

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


def process_tradelogs(path):
    trades = defaultdict(Trade)
    for chunk in pd.read_csv(path, chunksize=BATCH_SIZE):
        for row in chunk.itertuples():
            trade = Trade(tradeno=row.TRADENO, seccode=row.SECCODE, time=row.TIME,
                          buyorderno=row.BUYORDERNO, sellorderno=row.SELLORDERNO,
                          price=row.PRICE, volume=row.VOLUME)
            trades.setdefault(trade.tradeno, trade)

    return trades


if __name__ == '__main__':
    dates = ["301", "302", "305", "306", "307", "309", "312", "313", "314", "315", "316", "319", "320",
             "321", "322", "323", "326", "327", "328", "329", "330", "402", "403", "404", "405", "406",
             "409", "410", "411", "412", "413", "416", "417", "418", "420", "423", "424", "425", "426",
             "427", "428", "430", "502", "503", "504", "507", "508", "510", "511", "514", "515", "516",
             "517", "518", "521", "522", "523", "524", "525", "528", "529", "530", "531"]
    seccodes = ["USD000000TOD", "USD000UTSTOM", "EUR_RUB__TOD", "EUR_RUB__TOM", "EURUSD000TOD", "EURUSD000TOM"]
    seccodes4 = ["USD000UTSTOM", "EUR_RUB__TOM"]

    """
    date = "20180"
    for i in dates:
        for sec in seccodes:
            curr_date = date+i
            process_single_orderlog(path="input/OrderLog"+curr_date+".txt",seccode=sec,
                                    tradepath="input/TradeLog"+curr_date+".txt",
                                    outputpath="output/"+curr_date +sec+".txt" )
    """
    # for sec in seccodes4:
    #     process_single_orderlog(path="input/OrderLog20180301.txt", seccode=sec, tradepath="input/TradeLog20180301.txt",
    #                             outputpath="output/test.txt", day=1)
    date = "20180"
    # TASK4.1
    day = 1
    for i in dates:
        for sec in seccodes4:
            curr_date = date + i
            process_single_orderlog(path="input/OrderLog" + curr_date + ".txt",
                                    seccode=sec,
                                    tradepath="input/TradeLog" + curr_date + ".txt",
                                    outputpath="output/" + sec + "task4.txt",
                                    day=day)
        day += 1


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


