#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   api_connect_monitor.py
@Time    :   2020/03/26 15:28:32
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
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
import subprocess



logger = logging.getLogger()



class TraderSpi(traderapi.CTORATstpTraderSpi):
	def __init__(self,api,app):
		traderapi.CTORATstpTraderSpi.__init__(self)
		self.__api=api
		self.__req_id=0
		self.__app=app

	def OnFrontConnected(self):
		print("OnFrontConnected")
		self.__app.wake_up()
		

	def OnRspUserLogin(self, pRspUserLoginField, pRspInfo, nRequestID, bIsLast):
		print("OnRspUserLogin: ErrorID[%d] ErrorMsg[%s] RequestID[%d] IsLast[%d]" % (pRspInfo['ErrorID'], pRspInfo['ErrorMsg'], nRequestID, bIsLast))
		if pRspInfo['ErrorID'] == 0:
			self.__app.wake_up()
	
		
	def auto_increase_reqid(self):
		self.__req_id = self.__req_id + 1

	def test_req_user_login(self):
		#请求编号自增
		self.auto_increase_reqid()
		#请求登录
		login_req = traderapi.CTORATstpReqUserLoginField()
		login_req.LogInAccount=input("input login user:")
		login_req.LogInAccountType = traderapi.TORA_TSTP_LACT_UserID
		login_req.Password=input("input login password:")
		
		ret=self.__api.ReqUserLogin(login_req, self.__req_id)
		if ret!=0:
			print("ReqUserLogin ret[%d]" %(ret))
			self.__app.wake_up()
	

class TestApp(threading.Thread):
	def __init__(self, name, address):
		threading.Thread.__init__(self)
		self.__name = name
		self.__api = None
		self.__spi = None
		self.__address = address
		self.__lock = threading.Lock()
		self.__lock.acquire()

	def run(self):
		while True:
			if self.__api is None:
				print(traderapi.CTORATstpTraderApi_GetApiVersion())
				self.__api = traderapi.CTORATstpTraderApi.CreateTstpTraderApi()
				self.__spi = TraderSpi(self.__api, self)
				self.__api.RegisterSpi(self.__spi)
				self.__api.RegisterFront(self.__address)

				#订阅私有流
				self.__api.SubscribePrivateTopic(traderapi.TORA_TERT_RESTART)
				#订阅公有流
				self.__api.SubscribePublicTopic(traderapi.TORA_TERT_RESTART)

				#启动接口对象
				self.__api.Init()
			
			else:
				self.__lock.acquire()
				
				exit=False
				connect_err_count_dict = {}
				while True:
					print("start monitor trade api port")
					# ss -nap | grep 122.144.152.9:6500 | grep ESTAB
					para = '122.144.152.9:6500'
					connect_err_count_dict[para] = 0
					commond = 'ss -nap | grep ' + para + ' | grep ESTAB'
					execute_com = subprocess.Popen(commond,
											shell=True,
											stderr=subprocess.PIPE,
											stdout=subprocess.PIPE) # 执行命令
					res,err = execute_com.communicate()
					logger.info("res:" + str(res))
					print("res:" + str(res))
					print('len(res):', len(res))
					logger.info("err:" + str(err))
					print("err:" + str(err))
					if res!=b'':
						print('tcp ESTAB is ok')
						connect_err_count_dict[para] = 0
						time.sleep(3)
						continue
					else:
						print("tcp is not ESTAB!")
						connect_err_count_dict[para] += 1
						if connect_err_count_dict[para] < 3:
							print("send error msg!")
							#ct.send_sms_control('error')
						time.sleep(3)						
				
				if exit == True:
					break


	def wake_up(self):
		self.__lock.release()

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
	app=TestApp("thread", "tcp://122.144.152.9:6500")
	app.start()
	app.join()