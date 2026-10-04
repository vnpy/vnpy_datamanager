"""数据管理界面组件。"""
from collections.abc import Sequence
from functools import partial
from datetime import datetime, timedelta
from typing import cast

from vnpy.trader.ui import QtWidgets, QtCore
from vnpy.trader.engine import MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData
from vnpy.trader.database import DB_TZ
from vnpy.trader.utility import available_timezones

from ..engine import APP_NAME, ManagerEngine, BarOverview


INTERVAL_NAME_MAP: dict[Interval, str] = {
    Interval.MINUTE: "分钟线",
    Interval.HOUR: "小时线",
    Interval.DAILY: "日线",
}


class ManagerWidget(QtWidgets.QWidget):
    """数据管理主界面。"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """取得数据管理引擎并初始化界面。"""
        super().__init__()

        self.engine: ManagerEngine = cast(ManagerEngine, main_engine.get_engine(APP_NAME))

        self.init_ui()

    def init_ui(self) -> None:
        """搭建数据树、K 线表和刷新、导入、更新、下载按钮。"""
        self.setWindowTitle("数据管理")

        self.init_tree()
        self.init_table()

        refresh_button: QtWidgets.QPushButton = QtWidgets.QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh_tree)

        import_button: QtWidgets.QPushButton = QtWidgets.QPushButton("导入数据")
        import_button.clicked.connect(self.import_data)

        update_button: QtWidgets.QPushButton = QtWidgets.QPushButton("更新数据")
        update_button.clicked.connect(self.update_data)

        download_button: QtWidgets.QPushButton = QtWidgets.QPushButton("下载数据")
        download_button.clicked.connect(self.download_data)

        hbox1: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox1.addWidget(refresh_button)
        hbox1.addStretch()
        hbox1.addWidget(import_button)
        hbox1.addWidget(update_button)
        hbox1.addWidget(download_button)

        hbox2: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox2.addWidget(self.tree)
        hbox2.addWidget(self.table)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)

        self.setLayout(vbox)

    def init_tree(self) -> None:
        """创建数据概况树及其表头。"""
        labels: list = [
            "数据",
            "本地代码",
            "代码",
            "交易所",
            "数据量",
            "开始时间",
            "结束时间",
            "",
            "",
            ""
        ]

        self.tree: QtWidgets.QTreeWidget = QtWidgets.QTreeWidget()
        self.tree.setColumnCount(len(labels))
        self.tree.setHeaderLabels(labels)

    def init_table(self) -> None:
        """创建 K 线表格，列宽按内容调整。"""
        labels: list = [
            "时间",
            "开盘价",
            "最高价",
            "最低价",
            "收盘价",
            "成交量",
            "成交额",
            "持仓量"
        ]

        self.table: QtWidgets.QTableWidget = QtWidgets.QTableWidget()
        self.table.setColumnCount(len(labels))
        self.table.setHorizontalHeaderLabels(labels)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )

    def refresh_tree(self) -> None:
        """清空概况树，并按周期、交易所和合约重新挂上数据节点。"""
        self.tree.clear()

        # 初始化节点缓存字典
        interval_childs: dict[Interval, QtWidgets.QTreeWidgetItem] = {}
        exchange_childs: dict[tuple[Interval, Exchange], QtWidgets.QTreeWidgetItem] = {}

        # 查询数据汇总，并基于合约代码进行排序
        overviews: list[BarOverview] = self.engine.get_bar_overview()
        overviews.sort(key=lambda x: x.symbol)

        # 添加数据周期节点
        interval: Interval
        for interval in [Interval.MINUTE, Interval.HOUR, Interval.DAILY]:
            interval_child: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem()
            interval_childs[interval] = interval_child

            interval_name: str = INTERVAL_NAME_MAP[interval]
            interval_child.setText(0, interval_name)

        # 遍历添加数据节点
        overview: BarOverview
        for overview in overviews:
            interval = cast(Interval, overview.interval)
            exchange: Exchange = cast(Exchange, overview.exchange)
            start: datetime = cast(datetime, overview.start)
            end: datetime = cast(datetime, overview.end)

            # 获取交易所节点
            key: tuple[Interval, Exchange] = (interval, exchange)
            exchange_child: QtWidgets.QTreeWidgetItem | None = exchange_childs.get(key)

            if not exchange_child:
                interval_child = interval_childs[interval]

                exchange_child = QtWidgets.QTreeWidgetItem(interval_child)
                exchange_child.setText(0, exchange.value)

                exchange_childs[key] = exchange_child

            #  创建数据节点
            item: QtWidgets.QTreeWidgetItem = QtWidgets.QTreeWidgetItem(exchange_child)

            item.setText(1, f"{overview.symbol}.{exchange.value}")
            item.setText(2, overview.symbol)
            item.setText(3, exchange.value)
            item.setText(4, str(overview.count))
            item.setText(5, start.strftime("%Y-%m-%d %H:%M:%S"))
            item.setText(6, end.strftime("%Y-%m-%d %H:%M:%S"))

            output_button: QtWidgets.QPushButton = QtWidgets.QPushButton("导出")
            output_func: partial[None] = partial(
                self.output_data,
                overview.symbol,
                exchange,
                interval,
                start,
                end
            )
            output_button.clicked.connect(output_func)

            show_button: QtWidgets.QPushButton = QtWidgets.QPushButton("查看")
            show_func: partial[None] = partial(
                self.show_data,
                overview.symbol,
                exchange,
                interval,
                start,
                end
            )
            show_button.clicked.connect(show_func)

            delete_button: QtWidgets.QPushButton = QtWidgets.QPushButton("删除")
            delete_func: partial[None] = partial(
                self.delete_data,
                overview.symbol,
                exchange,
                interval
            )
            delete_button.clicked.connect(delete_func)

            self.tree.setItemWidget(item, 7, show_button)
            self.tree.setItemWidget(item, 8, output_button)
            self.tree.setItemWidget(item, 9, delete_button)

        # 展开顶层节点
        self.tree.addTopLevelItems(list(interval_childs.values()))

        for interval_child in interval_childs.values():
            interval_child.setExpanded(True)

    def import_data(self) -> None:
        """确认 CSV 导入参数后写入数据库并提示起止时间与条数，取消对话框则返回。"""
        dialog: ImportDialog = ImportDialog()
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return

        file_path: str = dialog.file_edit.text()
        symbol: str = dialog.symbol_edit.text()
        exchange: Exchange = dialog.exchange_combo.currentData()
        interval: Interval = dialog.interval_combo.currentData()
        tz_name: str = dialog.tz_combo.currentText()
        datetime_head: str = dialog.datetime_edit.text()
        open_head: str = dialog.open_edit.text()
        low_head: str = dialog.low_edit.text()
        high_head: str = dialog.high_edit.text()
        close_head: str = dialog.close_edit.text()
        volume_head: str = dialog.volume_edit.text()
        turnover_head: str = dialog.turnover_edit.text()
        open_interest_head: str = dialog.open_interest_edit.text()
        datetime_format: str = dialog.format_edit.text()

        start: datetime | None
        end: datetime
        count: int
        start, end, count = self.engine.import_data_from_csv(
            file_path,
            symbol,
            exchange,
            interval,
            tz_name,
            datetime_head,
            open_head,
            high_head,
            low_head,
            close_head,
            volume_head,
            turnover_head,
            open_interest_head,
            datetime_format
        )

        msg: str = f"\
        CSV载入成功\n\
        代码：{symbol}\n\
        交易所：{exchange.value}\n\
        周期：{interval.value}\n\
        起始：{start}\n\
        结束：{end}\n\
        总数量：{count}\n\
        "
        QtWidgets.QMessageBox.information(self, "载入成功！", msg, QtWidgets.QMessageBox.StandardButton.Ok)

    def output_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """选择时间区间和 CSV 路径后导出，文件被占用时提示失败。"""
        # Get output date range
        dialog: DateRangeDialog = DateRangeDialog(start, end)
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return
        start, end = dialog.get_date_range()

        # Get output file path
        path: str
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "导出数据",
            "",
            "CSV(*.csv)"
        )
        if not path:
            return

        result: bool = self.engine.output_data_to_csv(
            path,
            symbol,
            exchange,
            interval,
            start,
            end
        )

        if not result:
            QtWidgets.QMessageBox.warning(
                self,
                "导出失败！",
                "该文件已在其他程序中打开，请关闭相关程序后再尝试导出数据。"
            )

    def show_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """选择时间区间后，把 K 线填入表格。"""
        # Get output date range
        dialog: DateRangeDialog = DateRangeDialog(start, end)
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return
        start, end = dialog.get_date_range()

        bars: list[BarData] = self.engine.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        self.table.setRowCount(0)
        self.table.setRowCount(len(bars))

        row: int
        bar: BarData
        for row, bar in enumerate(bars):
            self.table.setItem(row, 0, DataCell(bar.datetime.strftime("%Y-%m-%d %H:%M:%S")))
            self.table.setItem(row, 1, DataCell(str(bar.open_price)))
            self.table.setItem(row, 2, DataCell(str(bar.high_price)))
            self.table.setItem(row, 3, DataCell(str(bar.low_price)))
            self.table.setItem(row, 4, DataCell(str(bar.close_price)))
            self.table.setItem(row, 5, DataCell(str(bar.volume)))
            self.table.setItem(row, 6, DataCell(str(bar.turnover)))
            self.table.setItem(row, 7, DataCell(str(bar.open_interest)))

    def delete_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> None:
        """确认后删除该合约和周期的全部 K 线，并提示删除条数。"""
        n: QtWidgets.QMessageBox.StandardButton = QtWidgets.QMessageBox.warning(
            self,
            "删除确认",
            f"请确认是否要删除{symbol} {exchange.value} {interval.value}的全部数据",
            QtWidgets.QMessageBox.StandardButton.Ok,
            QtWidgets.QMessageBox.StandardButton.Cancel
        )

        if n == QtWidgets.QMessageBox.StandardButton.Cancel:
            return

        count: int = self.engine.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        QtWidgets.QMessageBox.information(
            self,
            "删除成功",
            f"已删除{symbol} {exchange.value} {interval.value}共计{count}条数据",
            QtWidgets.QMessageBox.StandardButton.Ok
        )

    def update_data(self) -> None:
        """遍历已有 K 线概况，从各自结束时间继续下载，进度框可取消。"""
        overviews: list[BarOverview] = self.engine.get_bar_overview()
        total: int = len(overviews)
        count: int = 0

        dialog: QtWidgets.QProgressDialog = QtWidgets.QProgressDialog(
            "历史数据更新中",
            "取消",
            0,
            100
        )
        dialog.setWindowTitle("更新进度")
        dialog.setWindowModality(QtCore.Qt.WindowModality.WindowModal)
        dialog.setValue(0)

        overview: BarOverview
        for overview in overviews:
            if dialog.wasCanceled():
                break

            self.engine.download_bar_data(
                overview.symbol,
                cast(Exchange, overview.exchange),
                cast(Interval, overview.interval),
                cast(datetime, overview.end),
                self.output
            )
            count += 1
            progress: int = int(round(count / total * 100, 0))
            dialog.setValue(progress)

        dialog.close()

    def download_data(self) -> None:
        """打开历史数据下载对话框。"""
        dialog: DownloadDialog = DownloadDialog(self.engine)
        dialog.exec_()

    def show(self) -> None:
        """最大化显示窗口。"""
        self.showMaximized()

    def output(self, msg: str) -> None:
        """输出下载过程中的日志"""
        QtWidgets.QMessageBox.warning(
            self,
            "数据下载",
            msg,
            QtWidgets.QMessageBox.StandardButton.Ok,
            QtWidgets.QMessageBox.StandardButton.Ok,
        )


class DataCell(QtWidgets.QTableWidgetItem):
    """居中对齐的表格单元格。"""

    def __init__(self, text: str = "") -> None:
        """用给定文本创建居中对齐的单元格。"""
        super().__init__(text)

        self.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)


class DateRangeDialog(QtWidgets.QDialog):
    """选择数据起止时间的对话框。"""

    def __init__(self, start: datetime, end: datetime, parent: QtWidgets.QWidget | None = None) -> None:
        """用起止日期的下一天作为日期框初值。"""
        super().__init__(parent)

        self.setWindowTitle("选择数据区间")

        self.start_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                start.year,
                start.month,
                start.day + 1
            )
        )
        self.end_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                end.year,
                end.month,
                end.day + 1
            )
        )

        button: QtWidgets.QPushButton = QtWidgets.QPushButton("确定")
        button.clicked.connect(self.accept)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("开始时间", self.start_edit)
        form.addRow("结束时间", self.end_edit)
        form.addRow(button)

        self.setLayout(form)

    def get_date_range(self) -> tuple[datetime, datetime]:
        """返回开始时间，以及结束日期的下一天。"""
        start: datetime = cast(datetime, self.start_edit.dateTime().toPython())
        end: datetime = cast(datetime, self.end_edit.dateTime().toPython()) + timedelta(days=1)
        return start, end


class ImportDialog(QtWidgets.QDialog):
    """从 CSV 文件导入数据的对话框。"""

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        """创建 CSV 导入表单，周期列表不含 Tick。"""
        super().__init__()

        self.setWindowTitle("从CSV文件导入数据")
        self.setFixedWidth(300)

        self.setWindowFlags(
            (self.windowFlags() | QtCore.Qt.WindowType.CustomizeWindowHint)
            & ~QtCore.Qt.WindowType.WindowMaximizeButtonHint)

        file_button: QtWidgets.QPushButton = QtWidgets.QPushButton("选择文件")
        file_button.clicked.connect(self.select_file)

        load_button: QtWidgets.QPushButton = QtWidgets.QPushButton("确定")
        load_button.clicked.connect(self.accept)

        self.file_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.symbol_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.exchange_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        exchange: Exchange
        for exchange in Exchange:
            self.exchange_combo.addItem(str(exchange.name), exchange)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        interval: Interval
        for interval in Interval:
            if interval != Interval.TICK:
                self.interval_combo.addItem(str(interval.name), interval)

        self.tz_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.tz_combo.addItems(cast(Sequence[str], available_timezones()))
        self.tz_combo.setCurrentIndex(self.tz_combo.findText("Asia/Shanghai"))

        self.datetime_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("datetime")
        self.open_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("open")
        self.high_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("high")
        self.low_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("low")
        self.close_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("close")
        self.volume_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("volume")
        self.turnover_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("turnover")
        self.open_interest_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("open_interest")

        self.format_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("%Y-%m-%d %H:%M:%S")

        info_label: QtWidgets.QLabel = QtWidgets.QLabel("合约信息")
        info_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        head_label: QtWidgets.QLabel = QtWidgets.QLabel("表头信息")
        head_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        format_label: QtWidgets.QLabel = QtWidgets.QLabel("格式信息")
        format_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow(file_button, self.file_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(info_label)
        form.addRow("代码", self.symbol_edit)
        form.addRow("交易所", self.exchange_combo)
        form.addRow("周期", self.interval_combo)
        form.addRow("时区", self.tz_combo)
        form.addRow(QtWidgets.QLabel())
        form.addRow(head_label)
        form.addRow("时间戳", self.datetime_edit)
        form.addRow("开盘价", self.open_edit)
        form.addRow("最高价", self.high_edit)
        form.addRow("最低价", self.low_edit)
        form.addRow("收盘价", self.close_edit)
        form.addRow("成交量", self.volume_edit)
        form.addRow("成交额", self.turnover_edit)
        form.addRow("持仓量", self.open_interest_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(format_label)
        form.addRow("时间格式", self.format_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(load_button)

        self.setLayout(form)

    def select_file(self) -> None:
        """选择 CSV 文件并填入路径。"""
        result: tuple[str, str] = QtWidgets.QFileDialog.getOpenFileName(
            self, filter="CSV (*.csv)")
        filename: str = result[0]
        if filename:
            self.file_edit.setText(filename)


class DownloadDialog(QtWidgets.QDialog):
    """下载历史数据的对话框。"""

    def __init__(self, engine: ManagerEngine, parent: QtWidgets.QWidget | None = None) -> None:
        """创建下载表单，开始日期默认为三年前。"""
        super().__init__()

        self.engine: ManagerEngine = engine

        self.setWindowTitle("下载历史数据")
        self.setFixedWidth(300)

        self.symbol_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.exchange_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        exchange: Exchange
        for exchange in Exchange:
            self.exchange_combo.addItem(str(exchange.name), exchange)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        interval: Interval
        for interval in Interval:
            self.interval_combo.addItem(str(interval.name), interval)

        end_dt: datetime = datetime.now()
        start_dt: datetime = end_dt - timedelta(days=3 * 365)

        self.start_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                start_dt.year,
                start_dt.month,
                start_dt.day
            )
        )

        button: QtWidgets.QPushButton = QtWidgets.QPushButton("下载")
        button.clicked.connect(self.download)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("代码", self.symbol_edit)
        form.addRow("交易所", self.exchange_combo)
        form.addRow("周期", self.interval_combo)
        form.addRow("开始日期", self.start_date_edit)
        form.addRow(button)

        self.setLayout(form)

    def download(self) -> None:
        """Tick 周期下载 Tick，否则下载 K 线，并提示条数。"""
        symbol: str = self.symbol_edit.text()
        exchange: Exchange = Exchange(self.exchange_combo.currentData())
        interval: Interval = Interval(self.interval_combo.currentData())

        start_date: QtCore.QDate = self.start_date_edit.date()
        start: datetime = datetime(start_date.year(), start_date.month(), start_date.day())
        start = start.replace(tzinfo=DB_TZ)

        if interval == Interval.TICK:
            count: int = self.engine.download_tick_data(symbol, exchange, start, self.output)
        else:
            count = self.engine.download_bar_data(symbol, exchange, interval, start, self.output)

        QtWidgets.QMessageBox.information(self, "下载结束", f"下载总数据量：{count}条", QtWidgets.QMessageBox.StandardButton.Ok)

    def output(self, msg: str) -> None:
        """输出下载过程中的日志"""
        QtWidgets.QMessageBox.warning(
            self,
            "数据下载",
            msg,
            QtWidgets.QMessageBox.StandardButton.Ok,
            QtWidgets.QMessageBox.StandardButton.Ok,
        )
