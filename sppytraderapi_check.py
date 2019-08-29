# -*- coding: utf-8 -*-
"""
Created on 2019-08-26 15:38:05

@author: zhangwei
@comment:通过api连接系统，验证系统信息是否正确
"""

import sys
import sptraderapi
import time
import threading
import logging
import common_tools as ct
import getopt
import json
import datetime as dt


logger = logging.getLogger()
check_task=None




class SPTraderSpi(sptraderapi.CTORATstpSPTraderSpi):
    def __init__(self,api,app):
        sptraderapi.CTORATstpSPTraderSpi.__init__(self)
        self.__api=api
        self.__req_id=0
        self.__app=app
#        self.__cur_data=app.cur_data
        self.__res_list = []
        self.ndates = dt.datetime.now().strftime("%Y%m%d")

    def OnFrontConnected(self):
        #print("OnFrontConnected")
        self.__app.wake_up()
        

    def OnRspUserLogin(self, pRspUserLoginField, pRspInfo, nRequestID):
        msg = "OnRspUserLogin: ErrorID[%d] ErrorMsg[%s] RequestID[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID)
        print(msg)
        if pRspInfo['ErrorID'] == 0:
            self.__app.wake_up()
        else:
            #20190829增加的登陆失败也退出，脚本运行时不需要等待下次登陆处理。
            logger.error("Error:登陆失败 " + msg)
            self.__app.wake_up()
    
    def OnRspOrderInsert(self, pInputOrderField, pRspInfo, nRequestID):
        print("OnRspOrderInsert: ErrorID[%d] ErrorMsg[%s] RequestID[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID))
        print("\tInvestorID[%s] OrderRef[%d] OrderSysID[%s]" % (
                                                        pInputOrderField['InvestorID'],
                                                        pInputOrderField['OrderRef'],
                                                        pInputOrderField['OrderSysID']))
        self.__app.wake_up()

    def OnRtnOrder(self, pOrder):
        print("OnRtnOrder: InvestorID[%s] SecurityID[%s] OrderRef[%d] OrderLocalID[%s] Price[%.2f] VolumeTotalOriginal[%d] OrderSysID[%s] OrderStatus[%s]" % (
            pOrder['InvestorID'],
            pOrder['SecurityID'],
            pOrder['OrderRef'],
            pOrder['OrderLocalID'],
            pOrder['Price'],
            pOrder['VolumeTotalOriginal'],
            pOrder['OrderSysID'],
            pOrder['OrderStatus']))
        
    def OnRtnTrade(self, pTrade):
        print("OnRtnTrade: TradeID[%s] InvestorID[%s] SecurityID[%s] OrderRef[%d] OrderLocalID[%s] Price[%.2f] Volume[%d]" % (
            pTrade['TradeID'],
            pTrade['InvestorID'],
            pTrade['SecurityID'],
            pTrade['OrderRef'],
            pTrade['OrderLocalID'],
            pTrade['Price'],
            pTrade['Volume']
        ))

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo, nRequestID):
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
            print("SecurityID[%s] SecurityName[%s] UnderlyingSecurityID[%s] StrikeDate[%s]" % (
                                                            pSecurity['SecurityID'],
                                                            pSecurity['SecurityName'],
                                                            pSecurity['UnderlyingSecurityID'],
                                                            pSecurity['StrikeDate']))
            if pSecurity['SecurityID'] == self.__app.cur_data['SecurityID'] \
                and pSecurity['SecurityName'] != '' :
                msg = "OK,查询证券信息SecurityID为[%s]，SecurityName为[%s], StrikeDate为[%s]" \
                % (pSecurity['SecurityID'],pSecurity['SecurityName'],pSecurity['StrikeDate'])
                logger.info(msg)
            else:
                msg = "Error,查询证券信息SecurityID为[%s]，SecurityName为[%s], StrikeDate为[%s]" \
                % (pSecurity['SecurityID'],pSecurity['SecurityName'],pSecurity['StrikeDate'])
                logger.error(msg)
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
            
            
    def OnRspQryMarketData(self, pMarketData, pRspInfo, nRequestID, bIsLast):
        msg = "OnRspQryMarketData: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" %(
                                                            pRspInfo['ErrorID'],
                                                            pRspInfo['ErrorMsg'],
                                                            nRequestID,
                                                            bIsLast)
        logger.info(msg)
        if pRspInfo['ErrorID'] != 0:
            msg = "返回错误：错误信息为：%s" % pRspInfo['ErrorMsg']
            logger.error(msg)
            ct.send_sms_control('NoLimit',msg)
               
        if bIsLast!=1:
            print("cur_data:",self.__app.cur_data)
            self.__res_list.append(pMarketData)
            logger.info("SecurityID[%s] SecurityName[%s] TradingDay[%s] PreClosePrice[%s] OpenPrice[%s] LastPrice[%s] HighestPrice[%s] LowestPrice[%s] UpdateTime[%s]" % (
                                                            pMarketData['SecurityID'],
                                                            pMarketData['SecurityName'],
                                                            pMarketData['TradingDay'],
                                                            pMarketData['PreClosePrice'],
                                                            pMarketData['OpenPrice'],
                                                            pMarketData['LastPrice'],
                                                            pMarketData['HighestPrice'],
                                                            pMarketData['LowestPrice'],
                                                            pMarketData['UpdateTime']))
            
            if pMarketData['SecurityID'] == self.__app.cur_data['SecurityID'] and pMarketData['TradingDay'] == self.ndates \
                and pMarketData['PreClosePrice'] != 0.0 and pMarketData['UpperLimitPrice'] != 0.0 and pMarketData['LowerLimitPrice'] != 0.0 :
                msg = "OK,查询行情SecurityID[%s]，昨结算价为[%s], 涨停价为[%s], 跌停价为[%s],更新时间为:%s" \
                % (pMarketData['SecurityID'],pMarketData['PreClosePrice'],pMarketData['UpperLimitPrice'],pMarketData['LowerLimitPrice'],pMarketData['UpdateTime'])
                logger.info(msg)
            else:
                msg = "Error,查询行情SecurityID[%s]，昨结算价为[%s], 涨停价为[%s], 跌停价为[%s],更新时间为:%s" \
                % (pMarketData['SecurityID'],pMarketData['PreClosePrice'],pMarketData['UpperLimitPrice'],pMarketData['LowerLimitPrice'],pMarketData['UpdateTime'])
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg)
            
        else:
            if len(self.__res_list)==0:
                msg = "Error:查询无结果"
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg)
                #print("res_list:", self.__res_list)
            self.__app.wake_up()
            
        
    def auto_increase_reqid(self):
        self.__req_id = self.__req_id + 1;

    def test_req_user_login(self):
        #请求编号自增
        self.auto_increase_reqid()
        #请求登录
        login_req = sptraderapi.CTORATstpSPReqUserLoginField()
        #login_req.LogInAccount=input("input login user:")
        login_req.LogInAccount = self.__app.testData["LogInAccount"]
        if self.__app.testData["LogInAccountType"] == "TORA_TSTP_SP_LACT_UserID":
            login_req.LogInAccountType = sptraderapi.TORA_TSTP_SP_LACT_UserID
        #login_req.Password=input("input login password:")
        login_req.Password = self.__app.testData["password"]
        
        ret=self.__api.ReqUserLogin(login_req, self.__req_id)
        if ret!=0:
            print("ReqUserLogin ret[%d]" %(ret))
            self.__app.wake_up()

    def test_req_order_insert(self):
        #请求编号自增
        self.auto_increase_reqid()

        order_insert_field = sptraderapi.CTORATstpSPInputOrderField()

        #order_insert_field.ShareholderID = input("ShareholderID:")
        order_insert_field.ShareholderID = "A191117087"
        order_insert_field.SecurityID = "11012137"
        order_insert_field.ExchangeID = "1"
        order_insert_field.OrderRef = 0
        order_insert_field.OrderPriceType = sptraderapi.TORA_TSTP_SP_OPT_LimitPrice
        order_insert_field.Direction = sptraderapi.TORA_TSTP_SP_D_Buy
        order_insert_field.CombOffsetFlag = sptraderapi.TORA_TSTP_SP_OF_Open
        order_insert_field.CombHedgeFlag = sptraderapi.TORA_TSTP_SP_HF_Speculation
        order_insert_field.LimitPrice = '8.3'
        order_insert_field.VolumeTotalOriginal = 1
        order_insert_field.TimeCondition = sptraderapi.TORA_TSTP_SP_TC_GFD
        order_insert_field.VolumeCondition = sptraderapi.TORA_TSTP_SP_VC_AV
        
        

        ret=self.__api.ReqOrderInsert(order_insert_field, self.__req_id)
        if ret!=0:
            print("ReqOrderInsert ret[%d]" %(ret))
            self.__app.wake_up()

    def test_req_qry_security(self):
        #请求编号自增
        self.auto_increase_reqid()
        qry_security_field = sptraderapi.CTORATstpSPQrySecurityField()
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
        qry_trading_account_field = sptraderapi.CTORATstpSPQryTradingAccountField()
        ret=self.__api.ReqQryTradingAccount(qry_trading_account_field, self.__req_id)
        print("dict_data:", self.__app.cur_data)
        time.sleep(1)
        if ret!=0:
            print("ReqQryTradingAccount ret[%d]" %(ret))
            self.__app.wake_up()
        
    def test_req_qry_market_data(self):
        #for dict_data in self.__app.testData["QryMarketData"]:
        #请求编号自增
        self.auto_increase_reqid()
        qry_market_data_field = sptraderapi.CTORATstpSPQryMarketDataField()
        qry_market_data_field.SecurityID=self.__app.cur_data["SecurityID"]
        print("qry_market_data_field.SecurityID:",qry_market_data_field.SecurityID)
        print("dict_data:", self.__app.cur_data)
        ret=self.__api.ReqQryMarketData(qry_market_data_field, self.__req_id)
        #等待1秒给rsp足够的时间处理
        time.sleep(1)
        if ret!=0:
            print("ReqQryMarketData ret[%d]" %(ret))
            self.__app.wake_up()
    

class TestApp(threading.Thread):
        
    def __init__(self, name, info, task):
        threading.Thread.__init__(self)
        self.__name = name
        self.__api = None
        self.__spi = None        
        self.__address = info["address"]
        self.__lock = threading.Lock()
        self.__lock.acquire()
        self.testData = info
        self.__task = task
        self.cur_data = {}
        
    def run(self):

#        while True:
        print("self.__api", self.__api)
#            if self.__api is None:
        print(sptraderapi.CTORATstpSPTraderApi_GetApiVersion())
        self.__api = sptraderapi.CTORATstpSPTraderApi.CreateTstpSPTraderApi()
        self.__spi = SPTraderSpi(self.__api, self)
        self.__api.RegisterSpi(self.__spi)
        self.__api.RegisterFront(self.__address)
        #订阅私有流
        self.__api.SubscribePrivateTopic(sptraderapi.TORA_TERT_RESTART)
        #订阅公有流
        self.__api.SubscribePublicTopic(sptraderapi.TORA_TERT_RESTART)
        #启动接口对象
        self.__api.Init()

#            else:
        self.__lock.acquire()
        self.__spi.test_req_user_login()
#        time.sleep(1)

#            self.__spi.test_req_qry_security()
#            self.__spi.test_req_qry_trading_account()
        if self.__task == "qry_market_data":
            for dict_data in self.testData["QryMarketData"]:
                logger.info("excuting qry_market_data...")
                self.__lock.acquire()
                self.cur_data = dict_data
                self.__spi.test_req_qry_market_data()

        elif self.__task == "qry_security":
            for dict_data in self.testData["QrySecurity"]:
                logger.info("excuting qry_security...")
                self.__lock.acquire()
                self.cur_data = dict_data
                self.__spi.test_req_qry_security()
        else:
            logger.info("输入的任务名称无法识别！")

                   
    def wake_up(self):
        print("开始执行wakeup release")
        self.__lock.release()
        print("执行完成wakeup release")
        
    def stop(self):
        self.__running=False


def run_app(info, task):
    #启动线程
    #app=TestApp("thread", "tcp://122.144.152.9:8500")
    app=TestApp("thread", info, task)
    logger.info("init_login")
    app.start()
    app.join()



#def monitor_market_data_task():
#    print("执行Monitor任务")
#    check_task=1
#
#
#def monitor_qry_security_task():
#    print("执行Monitor任务")
#    check_task=3


def main(argv):
        
    try:
        yaml_path = './config/traderapi_monitor_logger.yaml'
        ct.setup_logging(yaml_path)

        with open('./config/sptraderapi_check.json', 'r') as f:
            Jsonlist = json.load(f)
            logger.debug(Jsonlist)
    
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('sppytraderapi_check.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('python trade_monitor.py -t <task>\n \
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
                
        if manual_task not in ["qry_market_data","qry_security","fpga","db_init","db_trade","errorLog","self_monitor","smss","sms0","sms100"]:
            logger.error("[task] input is wrong, please try again!")
            sys.exit()
            
        else:
            logger.info('manual_task is:%s' % manual_task)
            logger.info("Start to excute the api monitor")
            for info in Jsonlist:                
                run_app(info, manual_task)

    except Exception:
        logger.error('Faild to run api monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)

if __name__ == "__main__":
    main(sys.argv[1:])