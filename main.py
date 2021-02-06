import pandas as pd
from OrderBook import OrderBook
from Order import Order

BATCH_SIZE = 64


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
            if order.orderno % 100000 == 0:
                print(f"Processed {order.orderno} orders...")
            yield order

    print(f"No more rows in {path}")


def process_single_orderlog(path, seccode):
    orderbook = OrderBook(seccode=seccode)

    for order in read_batch(path=path,
                            batch_size=BATCH_SIZE):
        orderbook.post(order)

    print(f"Finished with reading {path}")


def process_all_orderlogs(path_to_folder) -> None:
    """
    Проходимся по папке с ордерлогами и вызываем "process_single_orderlog" на каждом ордерлоге
    :param path_to_folder: путь до папки с ордерлогами (и|или) трейдлогами
    """
    # TODO написать
    pass


if __name__ == '__main__':
    # process_all_orderlogs("folder")
    process_single_orderlog(path="input/OrderLog20180301.txt", seccode="USD")
