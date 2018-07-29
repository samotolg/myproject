import sys
import pandas as pd
import pandas_datareader.data as web
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import sqlite3 as sqlite
import time
import matplotlib.pyplot as plt
import mpl_finance as mpl
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.ticker as ticker

TR_REQ_TIME_INTERVAL = 0.2

class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self.kiwoom_connected = False
        self._create_kiwoom_instance()
        self._set_signal_slots()


    def _create_kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def _set_signal_slots(self):
        self.OnEventConnect.connect(self._event_connect)
        self.OnReceiveTrData.connect(self._receive_tr_data)

    def comm_connect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def set_input_value(self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):
        self.dynamicCall("CommRqData(QString, QString, int, QString", rqname, trcode, next, screen_no)
        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()
        return self.df

    def get_code_list_by_market(self, market):
        code_list = self.dynamicCall("getCodeListByMarket(QString)", market)
        code_list = code_list.split(';')
        return code_list[:-1]

    def get_code_name(self, code):
        name = self.dynamicCall("GetMasterCodeName(QString)", code)
        return name

    def _comm_get_data(self, code, real_type, field_name, index, item_name):
        ret = self.dynamicCall("CommGetData(QString, QString, QString, int, QString", code, real_type, field_name, index, item_name)
        return ret.strip()

    def _get_repeat_cnt(self, trcode, rqname):
        ret = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret

    def _opt10081(self, rqname, trcode):
        data_cnt = self._get_repeat_cnt(trcode, rqname)

        cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        lstValue = []
        dateValue = []
        for i in range(data_cnt):
            date = self._comm_get_data(trcode, "", rqname, i, "일자")
            open = self._comm_get_data(trcode, "", rqname, i, "시가")
            high = self._comm_get_data(trcode, "", rqname, i, "고가")
            low = self._comm_get_data(trcode, "", rqname, i, "저가")
            close = self._comm_get_data(trcode, "", rqname, i, "현재가")
            volume = self._comm_get_data(trcode, "", rqname, i, "거래량")
            lstValue.append([open, high, low, close, volume])
            dateValue.append(date)
            # print(date, open, high, low, close, volume)
        self.df = pd.DataFrame(lstValue, columns=cols, index = dateValue)

    def _event_connect(self, err_code):
        if err_code == 0:
            self.kiwoom_connected = True
            QMessageBox.about(self, "Information", "connected!")
        else:
            self.kiwoom_connected = False
            QMessageBox.about(self, "Information", "disconnected!")
        self.login_event_loop.exit()

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        if next == 2:
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == "opt10081_req":
            self._opt10081(rqname, trcode)

        try:
            self.tr_event_loop.exit()
        except AttributeError:
            pass

class MyWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setupUi()
        self.initialize()

    # def __exit__(self, exception_type, exception_value, traceback):
    #     self.db_con.close()

    def initialize(self):
        self.kiwoom_connected = False
        self.listRetrieved = False
        self.db_con = sqlite.connect("c:/work/project/stock_code.db")
        print("Initialized")


    def setupUi(self):
        self.setGeometry(200, 200, 1200, 600)
        self.setWindowTitle("주가정보")

        self.list = QListWidget()
        self.list.currentRowChanged.connect(self._listRowChanged)

        self.btnGetFromDB = QPushButton("DB 연결")
        self.btnGetFromDB.clicked.connect(self._btnConnectClickedDB)
        self.btnConnect = QPushButton("연결하기")
        self.btnConnect.clicked.connect(self._btnConnectClicked)
        self.btnGet = QPushButton("가져오기")
        self.btnGet.clicked.connect(self._btnGetClicked)
        self.btnStore = QPushButton("DB 저장하기")
        self.btnStore.clicked.connect(self._btnStoreClicked)
        self.btnDaily = QPushButton("일봉 가져오기")
        self.btnDaily.clicked.connect(self._btnDailyClicked)

        self.chart = plt.Figure()
        self.canvas = FigureCanvas(self.chart)

        leftLayout = QVBoxLayout()
        buttonLayout = QVBoxLayout()
        leftLayout.addWidget(self.list)
        buttonLayout.addWidget(self.btnGetFromDB)
        buttonLayout.addWidget(self.btnStore)
        buttonLayout.addWidget(self.btnConnect)
        buttonLayout.addWidget(self.btnGet)
        buttonLayout.addWidget(self.btnDaily)
        rightLayout = QVBoxLayout()
        rightLayout.addWidget(self.canvas)

        layout = QHBoxLayout()
        layout.addLayout(leftLayout)
        layout.addLayout(buttonLayout)
        layout.addLayout(rightLayout)
        layout.setStretchFactor(leftLayout, 0)
        layout.setStretchFactor(buttonLayout, 0)
        layout.setStretchFactor(rightLayout, 1)
        self.setLayout(layout)

    def _btnDailyClicked(self):
        if (self.listRetrieved == False):
            return
        selected = self.list.currentItem().text().split(":")
        print(selected)
        if selected == "":
            QMessageBox.about(self, "오류", "선택된 종목이 없습니다")
            return
        code = selected[0]
        name = selected[1]

        if (self.kiwoom_connected == False):
            return
        next = 0
        self.kiwoom.remained_data = True

        cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = pd.DataFrame(columns=cols)
        while (self.kiwoom.remained_data == True):
            time.sleep(TR_REQ_TIME_INTERVAL)
            self.kiwoom.set_input_value("종목코드", code)
            self.kiwoom.set_input_value("기준일자", "20180727")
            self.kiwoom.set_input_value("수정주가구분", 1)
            ndf = self.kiwoom.comm_rq_data("opt10081_req", "opt10081", next, "0101")
            df = df.append(ndf)
            next = 2
        print("모두 읽어왔습니다")
        #print (df)
        df.to_sql(code+":"+name, self.db_con, if_exists='replace')
        self.db_con.commit()
        self._draw_chart(df)

    def _draw_chart(self, df):
        data = df.head(30)
        date = pd.to_datetime(data.index)
        xlist = [v.strftime("%m/%d") for v in date]

        self.chart.clear()
        ax = self.chart.add_subplot(111)

        dlist = range(len(xlist))

        ax.xaxis.set_major_locator(ticker.FixedLocator(dlist))
        ax.xaxis.set_major_formatter(ticker.FixedFormatter(xlist))

        ax.invert_xaxis()

        mpl.candlestick2_ohlc(ax, data["Open"], data["High"], data["Low"] , data["Close"], width=0.5, colorup='r', colordown='b')
        #ax.legend(loc='best')
        ax.grid()
        self.canvas.draw()

    def _btnStoreClicked(self):
        if self.listRetrieved == False:
            return

        lstCode = []
        lstName = []
        for i in range(self.list.count()):
            item = self.list.item(i).text().split(":")
            lstCode.append(item[0])
            lstName.append(item[1])
        print(lstCode, lstName)
        data = pd.DataFrame({"Name" : lstName}, index=lstCode)
        print(data)
        data.to_sql("stock_code", self.db_con, if_exists='replace')
        self.db_con.commit()

        QMessageBox.about(self, "Information", "DB에 저장이 되었습니다")

    def _listRowChanged(self, item):
        text = self.list.currentItem().text().split(":")
        print(text[0] + "---" + text[1])

    def _btnConnectClickedDB(self):
        df = pd.read_sql("SELECT * FROM stock_code", self.db_con, index_col=None)
        cnt = df.__len__()
        for i in range(cnt):
            row = df.iloc[i]["index"] + ":" + df.iloc[i]["Name"]
            self.list.addItem(row)

    def _btnConnectClicked(self):
        self._create_kiwoom()
        self.kiwoom.comm_connect()
        self.kiwoom_connected = True

    def _btnGetClicked(self):
        if (self.kiwoom_connected == False or self.listRetrieved == True):
            return
        code_list = self.kiwoom.get_code_list_by_market('0')
        for code in code_list:
            name = self.kiwoom.get_code_name(code)
            self.list.addItem(code + ":" + name)
        self.listRetrieved = True

    def _create_kiwoom(self):
        self.kiwoom = Kiwoom()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = MyWindow()
    myWindow.show()
    app.exec_()
