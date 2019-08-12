# auto_monitor
auo monitor server and database by python script  
##脚本介绍  

1,monitor_status_task.py  
描述：通过ssh连接Linux服务器，实现了内存，硬盘，端口，线程的监控报警；实现了fpga文件目录下检查文件报警。  
执行参数说明：参数-l表示间隔，为0时单独执行1次，不输入的话默认是60秒，并只在交易时间监控basic,9:25之前监控fpga文件。  
参数-e表示执行监控的模块，'fpga'表示监控fpga目录文件，'basic'表示服务器基本信息监控,不输入表示2个都监控一次。  

 'monitor_status_task.py -l <loopsecends> -e <excute>\n \
                loopsecends=0 means no loop and just run once.\n \
                loopsecends=N means loop interval is N second. \n \
                (default:python monitor_status_task.py) means loop interval is 60 seconds. \n \
                excute=fpga means excute the fpgamonitor. \n \
                excute=basic means excute the basic monitor. \n \
                excute is Null means excute fpga and basic monitor.'  
配置文件：./config/server_status_config.txt--这个是监控服务器状态的配置文件。  
    注意：processes列，例如：chronyd|crond|dggg以"|"隔开，结尾不能带"|"。  
    ./config/fpga_config.txt--这个是fpga服务器的配置信息，配置地址和文件路径。  
日志：/mylog/server_status_run.log,错误日志：./mulog/status_error_log_yyyy-mm-dd.txt。  
监控结果：./monitor_result/status_result_yyyy-mm-dd.txt。  
监控标记文件：./flag_file/Basic_Monitor.fail or Basic_Monitor.success, FPAG_File_Monitor.success。  
2,ssh_normal_query.py  
描述：通过ssh连接Linux服务器，实现一些自定义的指令查询，并将结果保存到文件。  
配置文件：./config/normal_query_config.txt。  
日志：./normal_query/run_log_yyyy-mm-dd.txt。  
监控结果：./normal_query/normal_query_result_%Y%m%d%H%M%S.txt。  
3,monitor_errorLog.py  
描述：通过ssh连接Linux服务器，egrep指令搜索错误日志信息，并写入文件，并报警，报警后，ctrl+c退出程序，再次启动的话将不会报警。  
执行参数说明：启动脚本时可以输入循环的间隔，参数-l表示间隔，为0时单独执行1次，不输入的话默认是60秒，并只在交易时间监控。
'monitor_errorLog.py -l <loopsecends>   
                loopsecends=0 means no loop and just run once.  
                loopsecends=N means loop interval is N second.   
                (default:python monitor_errorLog.py) means loop interval is 60 seconds'  
配置文件：./config/server_logDir_config.txt。  
日志：/mylog/monitor_errorlog_run.log。  
监控结果：./mylog/errorLog_result_" + ndates + '.txt'。  
4，get_mssql_records.py  
描述：连接sqlserver2008数据库，自定义的查询记录，并将结果保存到文件。  
配置文件：一次性的目前写死在代码里，支持配置文件读取服务器信息;yaml_path = './config/mssql_records_logger.yaml'。  
日志：./mylog/db_query_run.log。  
记录文件：./db_records/tablename_yyyymmdd.txt。  
5，db_monitor.py  
描述：连接sqlserver2008数据库，盘前监控表的记录条数，日期字段，金额字段，匹配csv文件2个字段值比较。  
盘前加入了检查KHXX，csv上场文件和数据库的对比，比较客户资金是否一致，不一致检查出入金记录。  
盘中检查表数据是否增长；需要配置好table_check.json配置文件。  
执行参数说明：参数-l表示间隔，为0时单独执行1次，必须大于20s。不输入的话默认是600秒，并只在交易时间监控order表的增长,  
启动时执行一次before trade montor，检查盘前定义的监控内容。  
参数-e表示执行监控的模块，'before'表示监控盘前数据库检查，'trading'表示盘中数据库检查,可以不输入表示2个都监控一次。  
'db_monitor.py -l <loopsecends> -e <excute>\n \
                    loopsecends=0 means no loop and just run once.\n \
                    loopsecends=N means loop interval is N second. \n \
                    (default:python db_monitor.py) means loop interval is 600 seconds. \n \
                    excute=before means excute the before trade db monitor. \n \
                    excute=trading means excute the trading db monitor. \n \
                    excute is Null means excute before and trading db monitor.'  
配置文件：./config/table_check.json。  
日志：./mylog/db_monitor_run.log。  
监控结果：无。  
6，file_transfer.py  
描述：支持从windows到linux的上传下载文件，可以上传文件目录也可以上传单个文件，下载会保存到指定目录下以服务器IP地址命名的文件目录下。  
执行参数说明：参数-l表示间隔，为0时单独执行1次，必须大于20s。不输入的话默认是600秒，目录下载时不会取"."和".."开头的文件；  
支持单个文件下载，可以下载"."和".."开头的文件。  
启动时执行一次before trade montor，检查盘前定义的监控内容。  
参数-m，必填项，表示上传还是下载，'upload'表示上传，'download'表示下载。  
参数-w,可选项，输入windowd的本地目录，作为上传文件的本地目录或者下载的文件存放目录，  
    格式："D:\\my_project\\python"或者'/unload'。  
参数-l,可选项，输入Linux服务器的远程目录，作为上传文件的远程目录或者要下载的远程文件目录，  
    格式："/home/trade/temp"。  
参数-f,可选项，输入文件名字，可以对单个文件进行上传和下载，文件路径为上面参数的取值。  
 'file_transfer.py -m <method> -w <windir> -l <lindir> -f <filename>\n \
                        method=upload, means upload file from windows to linux.\n \
                        method=download, means download file from linux to windows. \n \
                        windir=dir, Window dir, The format like this: "D:\\my_project\\python". \n \
                        lindir=dir, Linux dir, The format like this: "/home/trade/temp". \n \
                        filename=filename, options- The single file to upload or download.''  
配置文件：./config/file_transfer_config.txt。  
日志：./mylog/file_transfer_run.log。  
监控结果：无。  
7，backup_file_check.py  
描述：检查备份当天文件是否存在，大小是否正常。再检查前一天的数据；  
正常的话删除2天前的文件，只保留2天的备份文件。周一删除5天前的文件，周二删除4天前的文件。  
执行参数说明：无  
配置文件：./config/backup_file_check_config.txt。  
日志：./mylog/backup_file_check_run.log。  
监控结果：无。  
8，get_order_count.py  
描述：统计csv文件中OrderTime列每秒的记录条数，并生成到count_values.csv文件中。  
执行参数说明：执行时需要输入要统计的csv文件名称，"python get_order_count.py order.csv"。   
配置文件：无。  
日志：无。  
监控结果：count_values.csv。  
9，check_xwdm.py  
描述：检查csv文件中xwdm列是否在本系统的配置文件中，config.txt配置文件中xwdm_check_col列为校验列，  
和csv配置文件列对应不对的将客户报出来。  
执行参数说明：无。   
配置文件：服务器配置：./config/check_xwdm_config.txt;  
服务器xwdm检查列表：./xwdm_check_list.csv，配置文件config.txt中的xwdm_check_col列对应于csv文件中的列。  
日志：./mylog/check_xwdm_run.log。  
监控结果：界面报警：OK or Error,并记录Log文件。  