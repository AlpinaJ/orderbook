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
            if (order.side % 1000000 == 0):
                print(f"Processed {order.side} orders...")
            yield order

    print(f"No more rows in {path}")


def process_single_orderlog(path, seccode, tradepath, outputpath):
    orderbook = OrderBook(seccode=seccode)
    spectrum = Spectrum(orderbook)
    bid_spectra = []
    ask_spectra = []
    trades = defaultdict(Trade)
    trades = process_tradelogs(tradepath)
    processed_trades = []
    file = open(outputpath, "a")
    file.write(seccode + "\n")

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):

        if order.seccode != seccode:
            continue

        if orderbook.current_timestamp < order.time:

            orderbook.collisions += orderbook.collision()

            current_bid_spectrum = spectrum.calculate_spectrum('B')
            bid_spectra.append([orderbook.current_timestamp, current_bid_spectrum])
            current_ask_spectrum = spectrum.calculate_spectrum('S')
            ask_spectra.append([orderbook.current_timestamp, current_ask_spectrum])

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

    spectrum.task3(bid_spectra, ask_spectra, file)
    #orderbook.task1(file)
    file.close()
    print(f"Finished with reading {path}")


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
    march_dates = ["01", "02", "05", "06", "07", "09", "12", "13", "14", "15", "16", "19", "20", "21", "22", "23", "26",
                   "27", "28", "29", "30"]
    april_dates = ["02", "03", "04", "05", "06", "09", "10", "11", "12", "13", "16", "17", "18", "20", "23", "24",
                   "25", "26    ", "27","28", "30"]
    may_dates = ["02", "03", "04", "07", "08", "10", "11", "14", "15", "16", "17", "18", "21", "22", "23", "24",
                   "25", "28", "29", "30", "31"]
    seccodes = ["USD000000TOD", "USD000UTSTOM", "EUR_RUB__TOD", "EUR_RUB__TOM","EURUSD000TOD", "EURUSD000TOM"]

    """
    date = "20180"
    for i in march_dates:
        for sec in seccodes:
            curr_date = date+"3"+i
            process_single_orderlog(path="input/OrderLog"+curr_date+".txt",seccode=sec,
                                    tradepath="input/TradeLog"+curr_date+".txt",
                                    outputpath="output/"+curr_date +sec+".txt" )
    for i in april_dates:
        for sec in seccodes:
            curr_date = date+"4"+i
            process_single_orderlog(path="input/OrderLog"+curr_date+".txt",seccode=sec,
                                    tradepath="input/TradeLog"+curr_date+".txt",
                                    outputpath="output/"+curr_date +sec+".txt" )
    for i in may_dates:
        for sec in seccodes:
            curr_date = date+"5"+i
            process_single_orderlog(path="input/OrderLog"+curr_date+".txt",seccode=sec,
                                    tradepath="input/TradeLog"+curr_date+".txt",
                                    outputpath="output/"+curr_date +sec+".txt" )

    """
    for sec in seccodes:
        process_single_orderlog(path="input/OrderLog20180301.txt", seccode=sec, tradepath="input/TradeLog20180301.txt", outputpath="output/test.txt")