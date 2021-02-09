import pandas as pd
from OrderBook import OrderBook
from Order import Order
from collections import defaultdict
from Trade import Trade

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
    trades = defaultdict(Trade)
    trades = process_tradelogs(tradepath)
    processed_trades= []

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):
        if order.time >= 235000000000:
            break
        if order.seccode != seccode:
            continue
        if orderbook.current_timestamp < order.time:
            orderbook.collisions += orderbook.collision()
            orderbook.current_timestamp = order.time
        if order.action == 0:
            orderbook.revoke(order.orderno, order.buysell)
        else:
            if order.action == 1:
                orderbook.post(order)
            else:
                if order.action == 2:
                    if order.tradeno in processed_trades:
                        continue
                    else:
                        orderbook.match(trades[order.tradeno])
                        processed_trades.append(order.tradeno)

    asks_keys = sorted(orderbook.asks.keys())
    bids_keys = sorted(orderbook.bids.keys())

    string = "Number of collisions is " + str(orderbook.collisions) + "\nAsks\n"
    file = open(outputpath, "a")
    file.write(string)

    # print(orderbook.asks)
    for price in asks_keys:
        total_volume = 0
        if orderbook.asks[price] is not None:
            for ordernumber in orderbook.asks[price]:
                total_volume += orderbook.orders[ordernumber]
            s = str(price) + " : " + str(total_volume) + "\n"
            file.write(s)

    file.write("\nBids\n")
    for price in bids_keys:
        total_volume = 0
        if orderbook.bids[price] is not None:
            for ordernumber in orderbook.bids[price]:
                total_volume += orderbook.orders[ordernumber]
            s = str(price) + " : " + str(total_volume) + "\n"
            file.write(s)
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
                   "25", "26", "27","28", "30"]
    may_dates = ["02", "03", "04", "07", "08", "10", "11", "14", "15", "16", "17", "18", "21", "22", "23", "24",
                   "25", "28", "29", "30", "31"]
    seccodes = ["USD000000TOD", "USD000UTSTOM", "EUR_RUB__TOD", "EUR_RUB__TOM","EURUSD000TOD", "EURUSD000TOM"]
    #process_single_orderlog(path="input/OrderLog1.txt", seccode="EUR_RUB__TOD", tradepath="input/TradeLog20180301.txt",
                           # outputpath="output/test.txt")

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
    #process_single_orderlog(path="input/OrderLog10.txt", seccode="EUR_RUB__TOD",tradepath="input/TradeLog10.txt", outputpath="output/test.txt")
