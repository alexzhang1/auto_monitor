#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import mdapi
import logging
import common_tools as ct
import getopt
import json
import datetime as dt
import time
import pandas as pd
import csv

logger = logging.getLogger()


class MdSpi(mdapi.CTORATstpMdSpi):
    def __init__(self,api, CheckData):
        mdapi.CTORATstpMdSpi.__init__(self)
        self.__api=api
        self.__req_id=1
        self.SubscribeList = CheckData['SubscribeMarketData']
        self.QuriyList = CheckData['QuiryMarketData']

    def auto_increase_reqid(self):
        self.__req_id = self.__req_id + 1;

    def OnFrontConnected(self):
        logger.info("OnFrontConnected")
        #请求登录
        login_req = mdapi.CTORATstpReqUserLoginField()
        self.__api.ReqUserLogin(login_req,self.__req_id) 

    def OnRspUserLogin(self, pRspUserLoginField, pRspInfo, nRequestID, bIsLast):
        logger.info("OnRspUserLogin: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID, bIsLast))
#        #订阅行情
#        sub_list=[b'000000']
#        self.__api.SubscribeMarketData(sub_list, mdapi.TORA_TSTP_EXD_COMM)
        #test
#        if pRspInfo['ErrorID'] == 0:
#            for list_dict in self.SubscribeList:
#                self.cur_data = list_dict
#                encode_str = list_dict["SecurityID"].encode('utf-8')
#                #ExchangeID = list_dict["ExchangeID"]
#                if list_dict["ExchangeID"] == 'SSE':
#                    ExchangeID = mdapi.TORA_TSTP_EXD_SSE
#                elif list_dict["ExchangeID"] == 'SZSE':
#                    ExchangeID = mdapi.TORA_TSTP_EXD_SZSE
#                elif list_dict["ExchangeID"] == 'HK':
#                    ExchangeID = mdapi.TORA_TSTP_EXD_HK
#                else:
#                    ExchangeID = mdapi.TORA_TSTP_EXD_COMM
#                sub_list=[]
#                sub_list.append(encode_str)
#                #订阅行情
#                #sub_list=[b'000001']
#                logger.info("sublist:")
#                logger.info(sub_list)
#                logger.info(ExchangeID)
#                self.__api.SubscribeMarketData(sub_list, ExchangeID)
#                time.sleep(2)
            

    def OnRspSubMarketData(self, pSpecificSecurity, pRspInfo, nRequestID, bIsLast):
        logger.info("OnRspSubMarketData")
        logger.info(pSpecificSecurity)
        logger.info(pRspInfo)


    def OnRtnDepthMarketData(self, pDepthMarketData):
#        pass
        logger.info("OnRtnDepthMarketData SecurityID[%s] TradingDay[%s] LastPrice[%.2f] Volume[%d] Turnover[%.2f] BidPrice1[%.2f] BidVolume1[%d] AskPrice1[%.2f] AskVolume1[%d]" % (pDepthMarketData['SecurityID'],
                                                                    pDepthMarketData['TradingDay'],
                                                                    pDepthMarketData['LastPrice'],
                                                                    pDepthMarketData['Volume'],
                                                                    pDepthMarketData['Turnover'],
                                                                    pDepthMarketData['BidPrice1'],
                                                                    pDepthMarketData['BidVolume1'],
                                                                    pDepthMarketData['AskPrice1'],
                                                                    pDepthMarketData['AskVolume1']))


    #查询行情快照
    def test_quiry_market_data(self):       

#        titelname = "TradingDay,SecurityID,ExchangeID,SecurityName,PreClosePrice,OpenPrice,Volume,Turnover,TradingCount,LastPrice,HighestPrice,LowestPrice,BidPrice1,AskPrice1,UpperLimitPrice,LowerLimitPrice,PERatio1,PERatio2,PriceUpDown1,PriceUpDown2,OpenInterest,BidVolume1,AskVolume1,\
#        BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,\
#        BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,UpdateTime,\
#        UpdateMillisec,ClosePrice,MDSecurityStat,HWFlag"
        titelname = "TradingDay,SecurityID,ExchangeID,SecurityName,PreClosePrice,OpenPrice,Volume,Turnover," \
            "TradingCount,LastPrice,HighestPrice,LowestPrice,BidPrice1,AskPrice1,UpperLimitPrice," \
            "LowerLimitPrice,PERatio1,PERatio2,PriceUpDown1,PriceUpDown2,OpenInterest,BidVolume1,AskVolume1," \
            "BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4," \
            "BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,UpdateTime," \
            "UpdateMillisec,ClosePrice,MDSecurityStat,HWFlag"   
        market_file = "./mylog/pMarketDataField.csv"
        ct.cover_write_file(market_file, titelname)
        quiry_MarketData_field = mdapi.CTORATstpInquiryMarketDataField()
        for list_dict in self.QuriyList:
            if list_dict["ExchangeID"] == 'SSE':
                ExchangeID = mdapi.TORA_TSTP_EXD_SSE
            elif list_dict["ExchangeID"] == 'SZSE':
                ExchangeID = mdapi.TORA_TSTP_EXD_SZSE
            elif list_dict["ExchangeID"] == 'HK':
                ExchangeID = mdapi.TORA_TSTP_EXD_HK
            else:
                ExchangeID = mdapi.TORA_TSTP_EXD_COMM
                           
            quiry_MarketData_field.SecurityID = list_dict["SecurityID"]
            quiry_MarketData_field.ExchangeID = ExchangeID
            #请求编号自增
            self.auto_increase_reqid()
#            time.sleep(1)
            ret=self.__api.ReqInquiryMarketDataMirror(quiry_MarketData_field, self.__req_id)
            if ret!=0:
                logger.warning("Error: [%s] ExchangeID[%s]请求返回错误，ReqInquiryMarketDataMirror ret[%d]" % (list_dict["SecurityID"], quiry_MarketData_field.ExchangeID,ret))
            else:
                logger.info("Ok: [%s] ExchangeID[%s]请求返回正确，ReqInquiryMarketDataMirror ret[%d]" % (list_dict["SecurityID"], quiry_MarketData_field.ExchangeID,ret))
#            time.sleep(1)

    #查询行情快照应答
    def OnRspInquiryMarketDataMirror(self, pMarketDataField, pRspInfo, nRequestID, bIsLast):
        logger.info("OnRspInquiryMarketDataMirror")
        logger.info(pMarketDataField)    
        market_file = "./mylog/pMarketDataField.csv"
        if (pMarketDataField != None):            
            data_list = list(pMarketDataField.values())
            ttl=[]
            for item in data_list:
                if(type(item)==bytes):
                    ttl.append(item.decode('utf-8'))
                else:
                    ttl.append(item)
            #print("ttl:", ttl)
            with open(market_file, 'a+', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(ttl)
        logger.info(pRspInfo)
        logger.info("OnRspInquiryMarketDataMirror: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID, bIsLast))


class mdapi_monitor():
    def __init__(self, CheckData):
#        self.task = check_task
        self.SubscribeList = CheckData['SubscribeMarketData']
        self.QuriyList = CheckData['QuiryMarketData']
        #连接服务器
        self.__address = CheckData['address']           
        logger.info(mdapi.CTORATstpMdApi_GetApiVersion())
        api = mdapi.CTORATstpMdApi_CreateTstpMdApi()
        self.__spi = MdSpi(api, CheckData)
        api.RegisterSpi(self.__spi)
        api.RegisterFront(self.__address)
        api.Init()
        #登陆等待1秒
        time.sleep(1)

    #监控比较marketdata文件
    def monitor_market_data(self):
        #先进行查询操作
        self.__spi.test_quiry_market_data()
        time.sleep(2)
        market_df = pd.read_csv("./mylog/pMarketDataField.csv", dtype=object)
#        print(market_df)
#        print(market_df.columns.size) #列数 
#        print(market_df.iloc[:,0].size)#行数   
        TrD_error_list=[]
        error_list=[]
        if market_df.iloc[:,0].size != 0:
            #判断TradingDay字段是否是当天日期           
            ndates = dt.datetime.now().strftime("%Y%m%d")
            nt_df = market_df[market_df['TradingDay'] != ndates]            
            if len(nt_df) == 0:
                logger.info("[TradingDay]的值%s和当天日期一致" % ndates)
            else:
                #print(nt_df)
                for row in nt_df.itertuples():
                    TrD_error_list.append(str(getattr(row, 'TradingDay')) + "::" + str(getattr(row, 'SecurityID')))
                msg = "Error: 服务器[%s]有证券行情TradingDay字段不是当天日期:[%s]" % (self.__address, ','.join(TrD_error_list))
                logger.info(msg)
                ct.send_sms_control("NoLimit",msg)
            #判断查询的合约行情是否都返回了结果
            
            if len(self.QuriyList) == market_df.iloc[:,0].size :
                logger.info("OK：行情查询返回记录条数和查询列表条数一致")
            else:                
                for dict_item in self.QuriyList:
    #                print("type1",type(dict_item['SecurityID']))
    #                print("type2",type(str(market_df.index[0])))
                    #df_list=list(market_df.index.map(lambda x: str(x)))
                    security_list=list(market_df['SecurityID'])   
                    #valuesDF['OrderTime'] = valuesDF['OrderTime'].map(lambda x: int(x)
    #                print("df_list:", df_list)                   
                    if dict_item['SecurityID'] in security_list:
                        logger.info("SecurityID [%s]查询行情成功" % dict_item['SecurityID'])
                    else:
                        logger.info("SecurityID [%s]查询行情没有返回" % dict_item['SecurityID'])
                        error_list.append(dict_item['SecurityID'])
                msg = "Error: 服务器[%s]mdapi行情查询 没有返回结果的SecurityID列表为：[%s]" % (self.__address, ",".join(error_list))
                logger.error(msg)
                ct.send_sms_control("NoLimit",msg)
        else:
            error_list = self.QuriyList
            msg = "Error:服务器[%s]mdapi行情查询 接口返回为空" % self.__address
            logger.info(msg)
            ct.send_sms_control("NoLimit",msg)
            
        if TrD_error_list == [] and error_list == []:
            msg = "Ok,服务器[%s]mdapi行情查询返回结果正确" % self.__address
            logger.info(msg)
            #ct.send_sms_control("NoLimit",msg)
            return 1
        else:
            return 0
        
           

def main(argv):
    
    try:
        yaml_path = './config/api_monitor_logger.yaml'
        ct.setup_logging(yaml_path)
        
        with open('./config/api_monitor_config.json', 'r') as f:
            JsonData = json.load(f)   
            
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('mdapi_check.py -t <task> or you can use -h for help')
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
            logger.warning("[task] input is wrong, please try again!")
            sys.exit()
            
        else:
            logger.info('manual_task is:%s' % manual_task)
            logger.info("Start to excute the mdapi monitor")          
            for PyMdApi_CheckData in JsonData['PyMdApi']:            
                if manual_task == 'qry_market_data':
                    logger.info("Start to excute the qry_market_data monitor")        
                    md_test = mdapi_monitor(PyMdApi_CheckData)
                    md_test.monitor_market_data()
    #                str = input("\n")
                else:
                    print("Input python mdapi_test.py -h for help")
        
    except Exception:
        logger.error('Faild to run mdapi monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)


if __name__ == "__main__":
    main(sys.argv[1:])
