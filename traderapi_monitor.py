# -*- coding: utf-8 -*-
"""
Created on 2019-08-26 15:38:05

@author: zhangwei
@comment:通过api连接系统，验证系统信息是否正确
"""

import sys
import traderapi
import time
import threading
import logging
import common_tools as ct
import getopt
import json
import datetime as dt
import pandas as pd
import csv



logger = logging.getLogger()




class TraderSpi(traderapi.CTORATstpTraderSpi):
    def __init__(self,api,app):
        traderapi.CTORATstpTraderSpi.__init__(self)
        self.__api=api
        self.__req_id=0
        self.__app=app
#        self.__cur_data=app.cur_data
        self.__res_list = []
        self.ndates = dt.datetime.now().strftime("%Y%m%d")
        #self.QuriyList = CheckData['QuiryMarketData']
        self.login_flag = 0

    def OnFrontConnected(self):
        #print("OnFrontConnected")
        self.__app.wake_up()
        

    def OnRspUserLogin(self, pRspUserLoginField, pRspInfo, nRequestID, bIsLast):
        msg = "OnRspUserLogin: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID, bIsLast)
        print(msg)
        if pRspInfo['ErrorID'] == 0:
            self.login_flag = 1
            #self.__app.wake_up()
        else:
            #20190829增加的登陆失败也退出，脚本运行时不需要等待下次登陆处理。
            logger.warning("Error:登陆失败 " + msg)
            self.login_flag = 0
            #self.__app.wake_up()
            
    
    def OnRspOrderInsert(self, pInputOrderField, pRspInfo, nRequestID, bIsLast):
        print("OnRspOrderInsert: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID, bIsLast))
        print("\tInvestorID[%s] OrderRef[%d] OrderSysID[%s]" % (
                                                        pInputOrderField['InvestorID'],
                                                        pInputOrderField['OrderRef'],
                                                        pInputOrderField['OrderSysID']))
        self.__app.wake_up()

    def OnRtnOrder(self, pOrder):
        print("OnRtnOrder: InvestorID[%s] SecurityID[%s] OrderRef[%s] OrderLocalID[%s] LimitPrice[%.2f] VolumeTotalOriginal[%d] OrderSysID[%s] OrderStatus[%s]" % (
            pOrder['InvestorID'],
            pOrder['SecurityID'],
            pOrder['OrderRef'],
            pOrder['OrderLocalID'],
            pOrder['LimitPrice'],
            pOrder['VolumeTotalOriginal'],
            pOrder['OrderSysID'],
            pOrder['OrderStatus']))
        
    def OnRtnTrade(self, pTrade):
        print("OnRtnTrade: TradeID[%s] InvestorID[%s] SecurityID[%s] OrderRef[%s] OrderLocalID[%s] Price[%.2f] Volume[%d]" % (
            pTrade['TradeID'],
            pTrade['InvestorID'],
            pTrade['SecurityID'],
            pTrade['OrderRef'],
            pTrade['OrderLocalID'],
            pTrade['Price'],
            pTrade['Volume']
        ))

#    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo, nRequestID):
    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        print("OnErrRtnOrderInsert")

    def OnRspQrySecurity(self, pSecurity, pRspInfo, nRequestID, bIsLast):
        msg = "OnRspQrySecurity: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" %(
                                                            pRspInfo['ErrorID'],
                                                            pRspInfo['ErrorMsg'],
                                                            nRequestID,
                                                            bIsLast)
        print(msg)

        if bIsLast!=1:
            print("cur_data:",self.__app.cur_data)
            self.__res_list.append(pSecurity)
            print("SecurityID[%s] SecurityName[%s] ProductID[%s] OpenDate[%s]" % (
                                                            pSecurity['SecurityID'],
                                                            pSecurity['SecurityName'],
                                                            pSecurity['ProductID'],
                                                            pSecurity['OpenDate']))
            if pSecurity['SecurityID'] == self.__app.cur_data['SecurityID'] \
                and pSecurity['SecurityName'] != '' :
                msg = "OK,查询证券信息SecurityID为[%s]，SecurityName为[%s], OpenDate为[%s]" \
                % (pSecurity['SecurityID'],pSecurity['SecurityName'],pSecurity['OpenDate'])
                logger.info(msg)
            else:
                msg = "Error,查询证券信息SecurityID为[%s]，SecurityName为[%s], OpenDate为[%s]" \
                % (pSecurity['SecurityID'],pSecurity['SecurityName'],pSecurity['OpenDate'])
                logger.warning(msg)
        else:
            if len(self.__res_list)==0:
                msg = "Error:查询无结果"
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg)
            self.__app.wake_up()

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        print("OnRspQryTradingAccount")
        if bIsLast!=1:
            print("AccountID[%s]" %(pTradingAccount['AccountID']))
        else:
            self.__app.wake_up()
 

    def OnRtnMarketStatus(self, pMarketStatus):
        print("OnRtnMarketStatus:", pMarketStatus)

    def OnRtnTransferFund(self, pTransferFund):
        print("OnRtnTransferFund:", pTransferFund)
           
    #查询实时行情应答
    def OnRspQryMarketData(self, pMarketData, pRspInfo, nRequestID, bIsLast):
        msg = "OnRspQryMarketData: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" %(
                                                            pRspInfo['ErrorID'],
                                                            pRspInfo['ErrorMsg'],
                                                            nRequestID,
                                                            bIsLast)
        logger.info(msg)
        print("pMarketData:",pMarketData)

        if pRspInfo['ErrorID'] != 0:
            msg = "返回错误：错误信息为：%s" % pRspInfo['ErrorMsg']
            logger.warning(msg)
            ct.send_sms_control('NoLimit',msg)
               
        if bIsLast!=1:
#            print("cur_data:",self.__app.cur_data)
#            self.__res_list.append(pMarketData)
#            logger.info("SecurityID[%s] SecurityName[%s] TradingDay[%s] PreClosePrice[%s] OpenPrice[%s] LastPrice[%s] HighestPrice[%s] LowestPrice[%s] UpdateTime[%s]" % (
#                                                            pMarketData['SecurityID'],
#                                                            pMarketData['SecurityName'],
#                                                            pMarketData['TradingDay'],
#                                                            pMarketData['PreClosePrice'],
#                                                            pMarketData['OpenPrice'],
#                                                            pMarketData['LastPrice'],
#                                                            pMarketData['HighestPrice'],
#                                                            pMarketData['LowestPrice'],
#                                                            pMarketData['UpdateTime']))
#            
#            if pMarketData['SecurityID'] == self.__app.cur_data['SecurityID'] and pMarketData['TradingDay'] == self.ndates \
#                and pMarketData['PreClosePrice'] != 0.0 and pMarketData['UpperLimitPrice'] != 0.0 and pMarketData['LowerLimitPrice'] != 0.0 :
#                msg = "OK,查询行情SecurityID[%s]，昨结算价为[%s], 涨停价为[%s], 跌停价为[%s],更新时间为:%s" \
#                % (pMarketData['SecurityID'],pMarketData['PreClosePrice'],pMarketData['UpperLimitPrice'],pMarketData['LowerLimitPrice'],pMarketData['UpdateTime'])
#                logger.info(msg)
#            else:
#                msg = "Error,查询行情SecurityID[%s]，昨结算价为[%s], 涨停价为[%s], 跌停价为[%s],更新时间为:%s" \
#                % (pMarketData['SecurityID'],pMarketData['PreClosePrice'],pMarketData['UpperLimitPrice'],pMarketData['LowerLimitPrice'],pMarketData['UpdateTime'])
#                logger.error(msg)
#                ct.send_sms_control('NoLimit',msg)
            if (pMarketData != None):            
                data_list = list(pMarketData.values())
                ttl=[]
                for item in data_list:
                    if(type(item)==bytes):
                        ttl.append(item.decode('utf-8'))
                    else:
                        ttl.append(item)
                with open(self.__app.QryMarketDataResFile, 'a+', encoding='utf-8', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(ttl)
            else:
                logger.warning("pMarketData数据为None")

            
#        else:
#            if len(self.__res_list)==0:
#                msg = "Error:查询无结果"
#                logger.error(msg)
#                ct.send_sms_control('NoLimit',msg)
#                #print("res_list:", self.__res_list)
#            self.__app.wake_up()
            
        
    def auto_increase_reqid(self):
        self.__req_id = self.__req_id + 1

    def test_req_user_login(self):
        logger.info("test_req_user_login...")
        #请求编号自增
        self.auto_increase_reqid()
        #请求登录
        login_req = traderapi.CTORATstpReqUserLoginField()
        #login_req.LogInAccount=input("input login user:")
        login_req.LogInAccount = self.__app.testData["LogInAccount"]
        if self.__app.testData["LogInAccountType"] == "TORA_TSTP_LACT_UserID":
            login_req.LogInAccountType = traderapi.TORA_TSTP_LACT_UserID
        #login_req.Password=input("input login password:")
        login_req.Password = self.__app.testData["password"]
        
        ret=self.__api.ReqUserLogin(login_req, self.__req_id)
        print("ReqUserLogin ret[%d]" %(ret))
        if ret!=0:
            #print("ReqUserLogin ret[%d]" %(ret))
            self.__app.wake_up()

    def test_req_order_insert(self):
        #请求编号自增
        self.auto_increase_reqid()

        order_insert_field = traderapi.CTORATstpInputOrderField()

        order_insert_field.SecurityID = input("SecurityID:")
        order_insert_field.OrderPriceType = traderapi.TORA_TSTP_OPT_LimitPrice
        order_insert_field.Direction = traderapi.TORA_TSTP_D_Buy
        order_insert_field.CombOffsetFlag = traderapi.TORA_TSTP_OF_Open
        order_insert_field.CombHedgeFlag = traderapi.TORA_TSTP_HF_Speculation
        order_insert_field.LimitPrice = float(input("LimitPrice:"))
        order_insert_field.VolumeTotalOriginal = int(input("VolumeTotalOriginal:"))
        order_insert_field.TimeCondition = traderapi.TORA_TSTP_TC_GFD
        order_insert_field.VolumeCondition = traderapi.TORA_TSTP_VC_AV
        order_insert_field.ExchangeID = input("ExchangeID:")
        #order_insert_field.ShareholderID = input("ShareholderID:")
        order_insert_field.ShareholderID = "0088078644"

        ret=self.__api.ReqOrderInsert(order_insert_field, self.__req_id)
        if ret!=0:
            print("ReqOrderInsert ret[%d]" %(ret))
            self.__app.wake_up()

    def test_req_qry_security(self):
        #请求编号自增
        self.auto_increase_reqid()
        qry_security_field = traderapi.CTORATstpQrySecurityField()
        #qry_security_field.SecurityID=input("SecurityID:")
        qry_security_field.SecurityID=self.__app.cur_data["SecurityID"]
        print("dict_data:", self.__app.cur_data)
        ret=self.__api.ReqQrySecurity(qry_security_field, self.__req_id)
        #等待1秒给rsp足够的时间处理
        time.sleep(1)
        if ret!=0:
            print("ReqQrySecurity ret[%d]" %(ret))
            self.__app.wake_up()

    def test_req_qry_trading_account(self):
        #请求编号自增
        self.auto_increase_reqid()
        qry_trading_account_field = traderapi.CTORATstpQryTradingAccountField()
        ret=self.__api.ReqQryTradingAccount(qry_trading_account_field, self.__req_id)
        print("dict_data:", self.__app.cur_data)
        time.sleep(1)
        if ret!=0:
            print("ReqQryTradingAccount ret[%d]" %(ret))
            self.__app.wake_up()
        
    def test_req_qry_market_data(self):
        #for dict_data in self.__app.testData["QuiryMarketData"]:
        titelname = "TradingDay,SecurityID,ExchangeID,SecurityName,PreClosePrice,OpenPrice,Volume,Turnover," \
            "TradingCount,LastPrice,HighestPrice,LowestPrice,BidPrice1,AskPrice1,UpperLimitPrice," \
            "LowerLimitPrice,PERatio1,PERatio2,PriceUpDown1,PriceUpDown2,OpenInterest,BidVolume1,AskVolume1," \
            "BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4," \
            "BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,UpdateTime," \
            "UpdateMillisec,ClosePrice,MDSecurityStat,HWFlag"
        ct.cover_write_file(self.__app.QryMarketDataResFile, titelname)
        #请求编号自增
        self.auto_increase_reqid()
        qry_market_data_field = traderapi.CTORATstpQryMarketDataField()
        
        for list_dict in self.__app.QuriyList:
            if list_dict["ExchangeID"] == 'SSE':
                ExchangeID = traderapi.TORA_TSTP_EXD_SSE
            elif list_dict["ExchangeID"] == 'SZSE':
                ExchangeID = traderapi.TORA_TSTP_EXD_SZSE
            elif list_dict["ExchangeID"] == 'HK':
                ExchangeID = traderapi.TORA_TSTP_EXD_HK
            else:
                ExchangeID = traderapi.TORA_TSTP_EXD_COMM
            qry_market_data_field.SecurityID = list_dict["SecurityID"]    
            qry_market_data_field.ExchangeID = ExchangeID
            ret = self.__api.ReqQryMarketData(qry_market_data_field, self.__req_id)
            #等待1秒给rsp足够的时间处理
    #        time.sleep(1)
            if ret!=0:
    #            print("ReqQryMarketData ret[%d]" %(ret))
                logger.warning("Error: [%s] ExchangeID[%s]请求返回错误，ReqQryMarketData ret[%d]" % (qry_market_data_field.SecurityID, qry_market_data_field.ExchangeID,ret))
                self.__app.wake_up()
            else:
                logger.info("Ok: [%s] ExchangeID[%s]请求返回正确，ReqQryMarketData ret[%d]" % (qry_market_data_field.SecurityID, qry_market_data_field.ExchangeID,ret))
                
    

class TestApp(threading.Thread):
        
    def __init__(self, name, task, CheckData):
        threading.Thread.__init__(self)
        self.__name = name
        self.__api = None
        self.__spi = None        
        self.__address = CheckData["address"]
        self.__lock = threading.Lock()
        self.__lock.acquire()
        self.testData = CheckData
        self.__task = task
        self.cur_data = {}
        self.QuriyList = CheckData['QuiryMarketData']
        self.QryMarketDataResFile = "./mylog/" + self.__address[6:] + "_trader_pMarketDataField.csv"
        self.check_flag = 0
        
    def run(self):

#        while True:
        #print("self.__api", self.__api)
#            if self.__api is None:
        print(traderapi.CTORATstpTraderApi_GetApiVersion())
        self.__api = traderapi.CTORATstpTraderApi.CreateTstpTraderApi()
        self.__spi = TraderSpi(self.__api, self)
        self.__api.RegisterSpi(self.__spi)
        self.__api.RegisterFront(self.__address)
        #订阅私有流
        #self.__api.SubscribePrivateTopic(traderapi.TORA_TERT_RESTART)
        #订阅公有流
        #self.__api.SubscribePublicTopic(traderapi.TORA_TERT_RESTART)
        #启动接口对象
        self.__api.Init()

#            else:
#        eventlet.monkey_patch()
#        with eventlet.Timeout(3,False):   #设置超时时间为2秒
#            print('这条语句正常执行')
#            #self.__lock.acquire()
#            time.sleep(5)
#            print('没有跳过这条输出')
        #self.__lock.acquire()
#        print("test5")
        self.__spi.test_req_user_login()
        time.sleep(2)

#            self.__spi.test_req_qry_security()
#            self.__spi.test_req_qry_trading_account()
        if self.__spi.login_flag == 1:
            if self.__task == "qry_market_data":
    #            for dict_data in self.testData["QryMarketData"]:
                logger.info("excuting qry_market_data...")
                self.__lock.acquire()
    #            self.cur_data = dict_data
                self.check_flag = self.monitor_market_data()
    
            elif self.__task == "qry_security":
                for dict_data in self.testData["QrySecurity"]:
                    logger.info("excuting qry_security...")
                    pass
                    #self.__lock.acquire()
                    #self.cur_data = dict_data
                    #self.__spi.test_req_qry_security()
            else:
                logger.warning("输入的任务名称无法识别！")
        else:
            logger.warning("Error:user_login登录失败，程序退出")
            sys.exit(1)


    #监控比较marketdata文件
    def monitor_market_data(self):
        #先进行查询操作
        self.__spi.test_req_qry_market_data()
        time.sleep(2)
        market_df = pd.read_csv(self.QryMarketDataResFile, dtype=object)
#        print(market_df)
#        print(market_df.columns.size) #列数 
#        print(market_df.iloc[:,0].size)#行数   
        TrD_error_list=[]
        error_list=[]
        if market_df.iloc[:,0].size != 0:
#            #判断TradingDay字段是否是当天日期           
#            ndates = dt.datetime.now().strftime("%Y%m%d")
#            nt_df = market_df[market_df['TradingDay'] != ndates]            
#            if len(nt_df) == 0:
#                logger.info("[TradingDay]的值%s和当天日期一致" % ndates)
#            else:
#                #print(nt_df)
#                for row in nt_df.itertuples():
#                    TrD_error_list.append(str(getattr(row, 'TradingDay')) + "::" + str(getattr(row, 'SecurityID')))
#                msg = "Error: 服务器[%s]有证券行情TradingDay字段不是当天日期:[%s]" % (self.__address, ','.join(TrD_error_list))
#                logger.info(msg)
#                ct.send_sms_control("NoLimit",msg)
            #判断查询的合约行情是否都返回了结果 
            #print("length:", len(self.QuriyList), market_df.iloc[:,0].size)
            if len(self.QuriyList) == market_df.iloc[:,0].size :
                logger.info("OK：行情查询返回记录条数和查询列表条数一致")
            else:                
                for dict_item in self.QuriyList:
                    security_list=list(market_df['SecurityID'])                     
                    if dict_item['SecurityID'] in security_list:
                        logger.info("SecurityID [%s]查询行情成功" % dict_item['SecurityID'])
                    else:
                        logger.info("SecurityID [%s]查询行情没有返回" % dict_item['SecurityID'])
                        error_list.append(dict_item['SecurityID'])
                msg = "Error: 服务器[%s] traderapi行情查询 没有返回结果的SecurityID列表为：[%s]" % (self.__address, ",".join(error_list))
                logger.error(msg)
                ct.send_sms_control("NoLimit",msg)
        else:
            error_list = self.QuriyList
            msg = "Error:服务器[%s] traderapi行情查询 接口返回为空" % self.__address
            logger.info(msg)
            ct.send_sms_control("NoLimit",msg)
            
        if TrD_error_list == [] and error_list == []:
            msg = "Ok,服务器[%s] traderapi行情查询 返回结果正确" % self.__address
            logger.info(msg)
            return 1
        else:
            return 0

                   
    def wake_up(self):
#        print("开始执行wakeup release")
        self.__lock.release()
#        print("执行完成wakeup release")
        
    def stop(self):
        self.__running=False


def run_app(task, CheckData):
    #启动线程
    #app=TestApp("thread", "tcp://122.144.152.9:8500")
    app=TestApp("thread", task, CheckData)
    logger.info("init_login")
    app.start()
    app.join()
    return app.check_flag



def main(argv):
        
    try:
        yaml_path = './config/api_monitor_logger.yaml'
        ct.setup_logging(yaml_path)

        with open('./config/api_monitor_config.json', 'r') as f:
            JsonData = json.load(f)
            logger.debug(JsonData)
    
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('sppytraderapi_check.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('python tradeapi_monitor.py -t <task>\n \
                    parameter -t comment: \n \
                    use -t can input the manul single task.\n \
                    task=["qry_market_data","mem","fpga","db_init","db_trade","errorLog"].  \n \
                    task="qry_market_data" means porcess and port monitor  \n \
                    task="qry_security" means memory monitor  \n \
                    task="db_trade" means db trading data monitor  \n \
                    task="errorLog" means file error log monitor  \n \
                    task="self_monitor" means self check monitor  \n \
                    task="smss" means check the sms send status  \n \
                    task="sms0" means set sms total_count=0  \n \
                    fpga_monitor and db_init_monitor just execute once on beginning ' )            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
                
        if manual_task not in ["qry_market_data","qry_security"]:
            logger.warning("[task] input is wrong, please try again!")
            sys.exit()
            
        else:
            logger.info('manual_task is:%s' % manual_task)
            logger.info("Start to excute the api monitor")
            TraderApi_CheckData = JsonData['PyTraderApi']
            res_flag = 0
            for CheckData in TraderApi_CheckData:                
                check_flag = run_app(manual_task, CheckData)
                res_flag += check_flag
            if res_flag == len(TraderApi_CheckData):
                msg = "Ok,所有服务器 traderapi行情查询 返回结果正确！"
                logger.info(msg)
                ct.send_sms_control("NoLimit", msg)
            else:
                logger.info("Error: 有服务器 traderapi行情查询 返回结果不正确！")
                

    except Exception:
        logger.error('Faild to run trade api monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)

if __name__ == "__main__":
    main(sys.argv[1:])