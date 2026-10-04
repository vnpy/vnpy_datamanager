import csv
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData

from vnpy_datamanager.engine import ManagerEngine


TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")
HEADER: str = (
    "symbol,exchange,datetime,open,high,low,close,volume,turnover,open_interest"
)


class RecordingDatabase:
    def __init__(self, bars: list[BarData] | None = None) -> None:
        self.stored: list[BarData] = list(bars or [])
        self.saved: list[tuple[list[BarData], bool]] = []
        self.loads: list[tuple[str, Exchange, Interval, datetime, datetime]] = []

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        copied: list[BarData] = list(bars)
        self.saved.append((copied, stream))
        self.stored.extend(copied)
        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> list[BarData]:
        # 与 sqlite 驱动一致：datetime 落在闭区间内才返回。
        self.loads.append((symbol, exchange, interval, start, end))
        matched: list[BarData] = [
            bar
            for bar in self.stored
            if bar.symbol == symbol
            and bar.exchange == exchange
            and bar.interval == interval
            and start <= bar.datetime <= end
        ]
        matched.sort(key=lambda bar: bar.datetime)
        return matched


def make_engine(
    monkeypatch: pytest.MonkeyPatch,
    database: RecordingDatabase,
) -> ManagerEngine:
    def get_database() -> RecordingDatabase:
        return database

    def get_datafeed() -> object:
        return object()

    monkeypatch.setattr("vnpy_datamanager.engine.get_database", get_database)
    monkeypatch.setattr("vnpy_datamanager.engine.get_datafeed", get_datafeed)
    return ManagerEngine(object(), object())  # type: ignore[arg-type]


def make_bar(
    when: datetime,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: float,
    turnover: float,
    open_interest: float,
) -> BarData:
    return BarData(
        gateway_name="DB",
        symbol="rb2501",
        exchange=Exchange.SHFE,
        datetime=when,
        interval=Interval.MINUTE,
        volume=volume,
        turnover=turnover,
        open_interest=open_interest,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
    )


def read_csv(path: Path) -> tuple[str, list[dict[str, str]]]:
    lines: list[str] = path.read_text(encoding="utf-8").splitlines()
    return lines[0], list(csv.DictReader(lines))


def test_import_csv_saves_parsed_bar_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database: RecordingDatabase = RecordingDatabase()
    engine: ManagerEngine = make_engine(monkeypatch, database)
    csv_path: Path = tmp_path / "bars.csv"
    csv_path.write_text(
        "datetime,open,high,low,close,volume,turnover\n"
        "2024-01-02 09:00:00,3500,3510,3490,3505,12,100.5\n"
        "2024-01-02 09:01:00,3506,3512,3501,3508,3,20\n",
        encoding="utf-8",
    )

    start: datetime | None
    end: datetime
    count: int
    start, end, count = engine.import_data_from_csv(
        str(csv_path),
        "rb2501",
        Exchange.SHFE,
        Interval.MINUTE,
        "Asia/Shanghai",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "open_interest",
        "%Y-%m-%d %H:%M:%S",
    )

    assert count == 2
    assert len(database.saved) == 1
    bars, stream = database.saved[0]
    assert stream is False
    assert len(bars) == 2
    first, second = bars
    assert first.symbol == "rb2501"
    assert first.exchange == Exchange.SHFE
    assert first.interval == Interval.MINUTE
    assert first.gateway_name == "DB"
    assert first.datetime.tzinfo == TZ
    assert first.datetime.replace(tzinfo=None) == datetime(2024, 1, 2, 9, 0)
    assert first.open_price == 3500
    assert first.high_price == 3510
    assert first.low_price == 3490
    assert first.close_price == 3505
    assert first.volume == 12
    assert first.turnover == 100.5
    assert first.open_interest == 0
    assert second.datetime.tzinfo == TZ
    assert second.datetime.replace(tzinfo=None) == datetime(2024, 1, 2, 9, 1)
    assert second.open_price == 3506
    assert second.high_price == 3512
    assert second.low_price == 3501
    assert second.close_price == 3508
    assert second.volume == 3
    assert second.turnover == 20
    assert second.open_interest == 0
    assert start == first.datetime
    assert end == second.datetime


def test_output_csv_writes_bars_in_range_and_skips_empty_range(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first: BarData = make_bar(
        datetime(2024, 1, 2, 9, 0, tzinfo=TZ),
        3500.0,
        3510.0,
        3490.0,
        3505.0,
        12.0,
        100.5,
        8.0,
    )
    second: BarData = make_bar(
        datetime(2024, 1, 2, 9, 1, tzinfo=TZ),
        3506.0,
        3512.0,
        3501.0,
        3508.0,
        3.0,
        20.0,
        9.0,
    )
    database: RecordingDatabase = RecordingDatabase([first, second])
    engine: ManagerEngine = make_engine(monkeypatch, database)
    full_start: datetime = datetime(2024, 1, 2, 0, 0, tzinfo=TZ)
    full_end: datetime = datetime(2024, 1, 2, 23, 59, tzinfo=TZ)
    empty_start: datetime = datetime(2024, 1, 1, 0, 0, tzinfo=TZ)
    empty_end: datetime = datetime(2024, 1, 1, 23, 59, tzinfo=TZ)
    full_path: Path = tmp_path / "full.csv"
    empty_path: Path = tmp_path / "empty.csv"

    full_ok: bool = engine.output_data_to_csv(
        str(full_path),
        "rb2501",
        Exchange.SHFE,
        Interval.MINUTE,
        full_start,
        full_end,
    )
    empty_ok: bool = engine.output_data_to_csv(
        str(empty_path),
        "rb2501",
        Exchange.SHFE,
        Interval.MINUTE,
        empty_start,
        empty_end,
    )

    assert database.loads == [
        ("rb2501", Exchange.SHFE, Interval.MINUTE, full_start, full_end),
        ("rb2501", Exchange.SHFE, Interval.MINUTE, empty_start, empty_end),
    ]
    full_header, full_rows = read_csv(full_path)
    empty_header, empty_rows = read_csv(empty_path)
    assert full_ok is True
    assert full_header == HEADER
    assert full_rows == [
        {
            "symbol": "rb2501",
            "exchange": "SHFE",
            "datetime": "2024-01-02 09:00:00",
            "open": "3500.0",
            "high": "3510.0",
            "low": "3490.0",
            "close": "3505.0",
            "volume": "12.0",
            "turnover": "100.5",
            "open_interest": "8.0",
        },
        {
            "symbol": "rb2501",
            "exchange": "SHFE",
            "datetime": "2024-01-02 09:01:00",
            "open": "3506.0",
            "high": "3512.0",
            "low": "3501.0",
            "close": "3508.0",
            "volume": "3.0",
            "turnover": "20.0",
            "open_interest": "9.0",
        },
    ]
    assert empty_ok is True
    assert empty_header == HEADER
    assert empty_rows == []
