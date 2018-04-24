# -*- coding: utf-8 -*-

# Author : hwijinkim , https://github.com/jeonghoonkang
# Author : jeonghoonkang , https://github.com/jeonghoonkang

# 가동수를 구하기 위한 py
# last modified by 20180122

# ver 0.3
# 1. Sock의 속도를 향상시킴

# 코드개선
# 1. P/G에서 sock을 한번만 실행
# 2. sock.sendall 두번하는 과정을 한번으로 축소
# 3. sock.close를 반복하지 않음 (put할 때 마다) - 최종 한번만 close 프로그램이 종료될 때
# 4. outlier & file log 기록하지 않음 - 프로그램의 속도를 최대화하기 위해
# 5. requests.get 예외처리 추가 (request error 방지, request가 힘들 경우 P/G이 끝나지 않고 계속 진행되도록

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import socket
import copy
import datetime
import time
from multiprocessing.reduction import send_handle
import sys
sys.path.insert(0, 'C:\Users\Be Pious\git\kdatahub\EE-data-analysis\lib')
sys.path.insert(0, 'C:\Users\Be Pious\git\kdatahub\EE-data-analysis\lib\tools')
sys.path.insert(0, 'C:\Users\Ine\Documents\GitHub\kdatahub\keti\kdatahub\EE-data-analysis\lib')
sys.path.insert(0, '/home/bornagain/kdatahub/EE-data-analysis/lib/tools')
sys.path.insert(0, '/home/bornagain/kdatahub/EE-data-analysis/lib')

import Jins_Utills
import in_list

USE_HTTP_API_PUT = False

#host  = '125.140.110.217'                                    # get할 url
#     port  = 4242                                                 # get할 port 번호
host  = 'tinyos.asuscomm.com'                                      # get할 url
port  = 44242                                                      # get할 port 번호
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((host, port))

def getTsdbData(tsdb_url, _try):
    try:
        getTsdbBuf = urllib2.urlopen(tsdb_url, timeout=1)
        read_buf = getTsdbBuf.read()
        read_buf = read_buf [1:-1]
        _dict = ast.literal_eval (read_buf);    #STR to DICT
        return 1,  _dict ['dps']

    except socket.timeout as e:
        print "< TIMEOUT!  url: %s, count: %s >" %(tsdb_url, _try + 1)
        if _try == 10:
            print "FAIL to getting TSDB data\n"
            a, b = None, '{}'
        else:
            a, b = getTsdbData(tsdb_url, _try + 1)
        return a, b

    except Exception as e:
        print "\n <There is no data in requested url: %s> \n" %(tsdb_url)
        print e, type (e)
        return None, '{}'

def pack_dps(metric, dps, tags):
    pack = []
    tag = {'metric': metric, 'tags': tags}

    for dp in dps:
        sdp = copy.copy(tag)
        sdp['timestamp'] = int(dp.encode('utf8'))
        sdp['value'] = dps[dp]
        pack.append(sdp)

    return pack

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def tsdb_query(query, start, end, metric, modem_num, holiday):

    _try = 0
    param = {}
    if start: param['start'] = start
    if end: param['end'] = end
    param['m'] = metric + '{' + 'holiday={1!s},modem_num={0!s}'.format(modem_num, holiday) + '}'

    try:
        response = requests.get(query, params=param)
        if response.ok:
            return response.json()

    except socket.timeout as e:
        _try += 1

        print "< TIMEOUT!  url: %s, count: %s >" %(host, _try)
        if _try == 10:
            print "FAIL to getting TSDB data\n"
            a = None
        else:
            response = requests.get(query, params=param)
            if response.ok:
                a = response.json()
        return a

    except Exception as e:
        _try += 1

        print "\n <There is no data in requested url: %s> \n" %(host)
        print e, type(e)
        return None

def tsdb_put(put, metric, dps, tags, file=None):
    packed_data = pack_dps(metric, dps, tags)
    for i in xrange(0, len(packed_data), 8):
        tmp = json.dumps(packed_data[i:i+8])

        if 0:
            response = requests.post(put, data=tmp)
        if response.text: print response.text
        else:
            try:
                response = requests_retry_session().post(put, data=tmp, timeout=5)
            except Exception as x:
                print 'requests timeout'
            else:
                if response.text: print response.text
    return

def tsdb_put_telnet(host, port, metric, dps, tags):

    send = ''
    packed_data = pack_dps(metric, dps, tags)
    for dp in packed_data:
        send += 'put'
        send += ' {0!s}'.format(dp['metric'])
        send += ' {0!r}'.format(dp['timestamp'])
        send += ' 1'                             # 값이 있으면 이진법 1처리(작동했다.)

        if 'tags' in dp:
            for key in dp['tags']:
                send += ' {0!s}'.format(key) + '={0!s}'.format(dp['tags'] [key])
        send += '\n'

    if len(send) > 0:
        sock.sendall(send)
        send = ''
    return

def sockWriteTSD(sock, __wmetric, __utime, __value, __tags = None):

    if __tags == None: __tags = 'ops=copy'

    _buf = "put %s %s %s %s\n" %( __wmetric, __utime, __value, __tags)

    try:
        ret = sock.sendall(_buf)
    except:
        print " something wroing in socket send function"
        pass
    time.sleep(0.0001)

    return

# outlier log 기록함수
def Chk_Outlier(chk_num, outlier_txt_name, adj_modem_num, unix_time, value):
    if chk_num == 1:
        f = open(outlier_txt_name, 'w')    # 경로설정 / 덮어쓰기
        f.write('%d:\n' % chk_num)
        f.write('modem_num : %s\n' % adj_modem_num)
        f.write('unix_time : %s\n' % ts2datetime(unix_time))
        f.write('value     : %s\n' % value)
        f.write('\n')
        f.close()
    else:
        f = open(outlier_txt_name, 'a')    # 경로설정 / 이어쓰기
        f.write('%d:\n' % chk_num)
        f.write('modem_num : %s\n' % adj_modem_num)
        f.write('unix_time : %s\n' % ts2datetime(unix_time))
        f.write('value     : %s\n' % value)
        f.write('\n')
        f.close()

def ts2datetime(ts_str):
    return datetime.datetime.fromtimestamp(int(ts_str)).strftime('%Y/%m/%d-%H:%M:%S')

def datetime2ts(dt_str):
    dt = datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")
    return time.mktime(dt.timetuple())

def str2datetime(dt_str):
    return datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")

def datetime2str(dt):
    return dt.strftime('%Y/%m/%d-%H:%M:%S')

def add_time(dt_str, days=0, hours=0, minutes=0, seconds=0):
    return datetime2str(str2datetime(dt_str) + datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds))

def is_past(dt_str1, dt_str2):
    return datetime2ts(dt_str1) < datetime2ts(dt_str2)

def is_weekend(dt_str):
    if str2datetime(dt_str).weekday() >= 5: return '1'
    else: return '0'

if __name__ == '__main__':
    # 1. 설정값 입력 - 시작
    query = 'http://{0!s}:{1!r}/api/quer05y'.format(host, port)          # opentsdb에서 get할 url 주소
    put   = 'http://{0!s}:{1!r}/api/put?summary'.format(host, port)    # opentsdb에서 put할 url 주소
    first = '2016/07/01-00:00:00'                                      # get 시작일
    last  = '2016/07/02-00:00:00'                                      # get 종료일
    metric_in  = 'rc05_excel_copy_tag_v11'                        # get할 metric명
    metric_out = 'rc06_operation_rate_v18'                             # put할 metric명
    cnt = 0
    # 2. get할 metric에서 가져올 특정 list 설정
    modem_list = in_list.modem_all_list
    print '<총 {0!r}개의 Modem list 데이터 추출>\n'.format(len(modem_list))

    start_time = datetime.datetime.now()        # 프로그램 시작값
    #sys.exit("finish")
    #3. 입력된 lte_list를 for문으로 하나씩 get함
    #4. get한 값을 put함
    #5. 반복적으로 get & put 마지막 list까지
    ### 시작일 ~ 끝일까지 15분 data씩 get & put (get data 많으면 opentsdb가 timeout될 수 있기 때문에)

    # adding 1days 증가 하면서 지정 날짜까지 Loop 실행
    end = first
    while is_past(end, last):
        tmp_time = datetime.datetime.now()
        start = end
        end = add_time(start, days=1)

        print end, last

        for modem in modem_list:
            #cnt += 1
            #print '<{0!r}/{1!r}개 Modem list 처리중>\n'.format(cnt,len(modem_list))
            modem_holiday = is_weekend(start)
            query_result = tsdb_query(query, start, end, metric_in, modem, modem_holiday)
            # query_result의 값이 none or []일때 continue - 예외처리
            if query_result == None or query_result == []: continue

            group = query_result[0]

            # group의 값을 받아서 가공 후 put하는 함수
            tsdb_put_telnet(host, port, metric_out, group['dps'], group['tags'])
        print 'elapsed time: {0!s}, {1!s}'.format(datetime.datetime.now() - tmp_time, datetime.datetime.now() - start_time)
        #print '<총 {0!r}~{1!r}날 Modem 데이터 처리>\n'.format(end, last)
    print 'total elapsed time: {0!s}'.format(datetime.datetime.now() - start_time)
