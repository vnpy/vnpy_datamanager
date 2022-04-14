import csv
from datetime import datetime
from typing import List, Tuple

from pytz import timezone

from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, TickOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed


APP_NAME = "DataManager"


class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> Tuple:
        """"""
        with open(file_path, "rt") as f:
            buf = [line.replace("\0", "") for line in f]

        reader = csv.DictReader(buf, delimiter=",")

        bars = []
        start = None
        count = 0
        tz = timezone(tz_name)

        for item in reader:
            if datetime_format:
                dt = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt = datetime.fromisoformat(item[datetime_head])
            dt = tz.localize(dt)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        if interval == Interval.TICK:
            ticks: List[TickData] = self.load_tick_data(symbol, exchange, start, end)
            
            fieldnames: list = [
                "symbol",
                "exchange",
                "datetime",
                "open",
                "high",
                "low",
                "pre_close",
                "volume",
                "turnover",
                "open_interest",
                "last_price",
                "last_volume",
                "limit_up",
                "limit_down",
                "bid_price_1",
                "bid_price_2",
                "bid_price_3",
                "bid_price_4",
                "bid_price_5",
                "ask_price_1",
                "ask_price_2",
                "ask_price_3",
                "ask_price_4",
                "ask_price_5",
                "bid_volume_1",
                "bid_volume_2",
                "bid_volume_3",
                "bid_volume_4",
                "bid_volume_5",
                "ask_volume_1",
                "ask_volume_2",
                "ask_volume_3",
                "ask_volume_4",
                "ask_volume_5",
                "localtime"
            ]
        else:
            bars: List[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)
    
            fieldnames: list = [
                "symbol",
                "exchange",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "open_interest"
            ]

        try:
            with open(file_path, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                if interval == Interval.TICK:
                    for tick in ticks:
                        d: dict = {
                            "symbol": tick.symbol,
                            "exchange": tick.exchange.value,
                            "datetime": tick.datetime.strftime("%Y-%m-%d %H:%M:%S.%f"),
                            "open": tick.open_price,
                            "high": tick.high_price,
                            "low": tick.low_price,
                            "pre_close": tick.pre_close,
                            "turnover": tick.turnover,
                            "volume": tick.volume,
                            "open_interest": tick.open_interest,
                            "last_price": tick.last_price,
                            "last_volume": tick.last_volume,
                            "limit_up": tick.limit_up,
                            "limit_down": tick.limit_down,
                            "bid_price_1": tick.bid_price_1,
                            "bid_price_2": tick.bid_price_2,
                            "bid_price_3": tick.bid_price_3,
                            "bid_price_4": tick.bid_price_4,
                            "bid_price_5": tick.bid_price_5,
                            "ask_price_1": tick.ask_price_1,
                            "ask_price_2": tick.ask_price_2,
                            "ask_price_3": tick.ask_price_3,
                            "ask_price_4": tick.ask_price_4,
                            "ask_price_5": tick.ask_price_5,
                            "bid_volume_1": tick.bid_volume_1,
                            "bid_volume_2": tick.bid_volume_2,
                            "bid_volume_3": tick.bid_volume_3,
                            "bid_volume_4": tick.bid_volume_4,
                            "bid_volume_5": tick.bid_volume_5,
                            "ask_volume_1": tick.ask_volume_1,
                            "ask_volume_2": tick.ask_volume_2,
                            "ask_volume_3": tick.ask_volume_3,
                            "ask_volume_4": tick.ask_volume_4,
                            "ask_volume_5": tick.ask_volume_5,
                        }
                        if tick.localtime:
                            d["localtime"] = tick.localtime.strftime("%Y-%m-%d %H:%M:%S.%f")
                        writer.writerow(d)
                else:
                    for bar in bars:
                        d: dict = {
                            "symbol": bar.symbol,
                            "exchange": bar.exchange.value,
                            "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                            "open": bar.open_price,
                            "high": bar.high_price,
                            "low": bar.low_price,
                            "close": bar.close_price,
                            "turnover": bar.turnover,
                            "volume": bar.volume,
                            "open_interest": bar.open_interest,
                        }
                        writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> List[BarOverview]:
        """"""
        return self.database.get_bar_overview()

    def get_tick_overview(self) -> List[TickOverview]:
        """"""
        if hasattr(self.database, 'get_tick_overview'):
            return self.database.get_tick_overview()
        else:
            return []

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count = self.database.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """"""
        ticks = self.database.load_tick_data(
            symbol,
            exchange,
            start,
            end
        )

        return ticks

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """"""
        count = self.database.delete_tick_data(
            symbol,
            exchange
        )

        return count

    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: str,
        start: datetime
    ) -> int:
        """
        Query bar data from datafeed.
        """
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=datetime.now(DB_TZ)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        contract = self.main_engine.get_contract(vt_symbol)

        # If history data provided in gateway, then query
        if contract and contract.history_data:
            data = self.main_engine.query_history(
                req, contract.gateway_name
            )
        # Otherwise use datafeed to query data
        else:
            data = self.datafeed.query_bar_history(req)

        if data:
            self.database.save_bar_data(data)
            return(len(data))

        return 0

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime
    ) -> int:
        """
        Query tick data from datafeed.
        """
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=datetime.now(DB_TZ)
        )

        data = self.datafeed.query_tick_history(req)

        if data:
            self.database.save_tick_data(data)
            return(len(data))

        return 0
