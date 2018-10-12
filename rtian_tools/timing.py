# coding=utf8
import re
import time
import os
import logging
import functools
import threading


PATH_EXEC = "exec"
PATH_TIMING = "timing"


def analy_cron(cron, kwargs):
    '''
    Parse the temporal expression and return a dictionary list .
    :param cron: For more information, please refer to **********
    :param start_time: 某个时间之后
    :return: a dictionary list .
    '''
    # results = []
    # 0 0 9 1 * ?
    _re = re.search('(\S+)\s(\S+)\s(\S+)\s(\S+)\s\*\s\?', cron)
    _re1 = re.search('(\S+)\s(\S+)\s(\S+)\s\?\s\*\s(\S+)', cron)
    if _re:

        def generate_cron(_second, _minute, _hour, _day):
            return {
                "_second": _second,
                "_minute": _minute,
                "_hour": _hour,
                "expression": "{_second} {_minute} {_hour} {_day} * ?".format(
                    _second=_second, _minute=_minute, _hour=_hour, _day=_day
                )
            }

        def analy_hours(_second, _minute, _hours, _day):
            results = []
            if len(_hours) != 1:
                for _hour in _hours:

                    if int(_hour) < int(time.strftime("%H")):
                        continue

                    results.append(generate_cron(_second, _minute, _hour, "*"))
                return results
            else:
                _hour = _hours[0]

            _hours = _hour.split("/")
            if len(_hours) == 1:
                _hour = _hours[0]
                return [generate_cron(_second, _minute, _hour, _day)]
            else:
                _hour1 = int(_hours[0])
                _hour2 = int(_hours[1])
                # print _hour1, _hour2

                for _h in range(0, 24):
                    time_str = "{0}-{1}-{2} {3}:{4}:{5}".format(
                        time.strftime("%Y"),
                        time.strftime("%m"),
                        time.strftime("%d"),
                        _h, _minute, _second
                    )
                    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    timeStamp = int(time.mktime(timeArray))
                    if timeStamp < kwargs.get("start_time", 0):
                        continue
                    timeIncrement = int((timeStamp - kwargs.get("start_time", 0)) / 60 / 60)
                    if timeIncrement % _hour2 != 0 or \
                            (timeIncrement == 0 and _hour2 != 1) or \
                            (timeIncrement < _hour1):
                        continue
                    if _h < int(time.strftime("%H")):
                        continue

                    if _h > int(time.strftime("%H")):
                        continue

                    # print _h

                    results.append(generate_cron(_second, _minute, _h, _day))

            return results

        def analy_minute(results):
            nresults = []
            for result in results:
                _second = result["_second"]
                _minute = result["_minute"]
                _hour = result.get("_hour", "*")
                _day = result.get("_day", "*")
                for _minutei in _minute.split(","):
                    if int(_minutei) < int(time.strftime("%M")) and int(_minutei) != 0:
                        continue
                    nresults.append(generate_cron(_second, _minutei, _hour, _day))
            return nresults

        results = []
        _second_ = _re.group(1)
        _minute_ = _re.group(2)
        _hours_ = _re.group(3).split(",")
        _day_ = _re.group(4).split(",")
        if len(_day_) != 1:
            for _dayi_ in _day_:
                if int(_dayi_) != int(time.strftime("%d")):
                    continue
                results += analy_hours(_second_, _minute_, _hours_, _dayi_)
        else:
            _day_ = _day_[0]
            if _day_.isdigit():
                if int(_day_) == int(time.strftime("%d")):
                    results += analy_hours(_second_, _minute_, _hours_, _day_)
            else:
                results += analy_hours(_second_, _minute_, _hours_, _day_)

        results = analy_minute(results)
        return results
    elif _re1:
        def generate_cron(_second, _minute, _hour, _week):
            return {
                "_second": _second,
                "_minute": _minute,
                "_hour": _hour,
                "expression": "{_second} {_minute} {_hour} ? * {_week}".format(
                    _second=_second, _minute=_minute, _hour=_hour, _week=_week
                )
            }

        _second = _re1.group(1)
        _minute = _re1.group(2)
        _hour = _re1.group(3)
        _week = _re1.group(4)

        results = []

        if _week.upper() != time.strftime("%a").upper():
            return results

        if int(_hour) > int(time.strftime("%H")):
            return results

        results.append(generate_cron(_second, _minute, _hour, _week))

        return results
    else:
        raise Exception("The cron expression is error !")


def check_time_expression(cron_dict):

    expression = "%Y-%m-%d %H:%M:%S"
    nowyear = time.strftime("%Y")
    nowmonth = time.strftime("%m")
    nowday = time.strftime("%d")
    nowhour = time.strftime("%H")
    nowminute = time.strftime("%M")

    _month = cron_dict.get("_month", nowmonth)
    _day = cron_dict.get("_day", nowday)
    _hour = cron_dict.get("_hour", nowhour)
    _minute = cron_dict.get("_minute", nowminute)

    righttime = time.mktime(
        time.strptime(
            "{year}-{month}-{day} {hour}:{minute}:00".format(
                year=nowyear, month=_month, day=_day, hour=_hour, minute=_minute
            ),
            expression
        )
    )
    nowtime = time.time()

    if nowtime > righttime:
        return True
    else:
        return False


def check_timing(task_name, expression, **kwargs):
    '''
    Determines whether the current time satisfies the cron expression .
    :param task_name: The name of the scheduled task .
    :param expression: Cron expression .
    :return: True or False .
    '''
    path = kwargs.get("path", PATH_TIMING)

    filename = path + "/" + str(task_name) + ".timing"
    if not os.path.exists(path):
        os.mkdir(path)

    try:
        with open(filename, 'r') as fp:
            content = fp.read()
    except:
        content = ""

    crons = analy_cron(expression, kwargs)
    for cron_dict in crons:
        dokey = "<{0} ({1})>".format(time.strftime("%Y-%m-%d"), cron_dict["expression"])
        if check_time_expression(cron_dict) and dokey not in content:
            with open(filename, "ab") as fp:
                fp.write(dokey.encode() + "\n".encode())

            return True

    return False


def set_survival(name, isstop=True, path=PATH_EXEC):
    content = ""
    if isstop:
        content = "stop"

    with open(path + "/" + name + ".survival", "wb") as fp:
        fp.write(content.encode())


def get_survival(name, path=PATH_EXEC):
    try:
        with open(path + "/" + name + ".survival", "r") as fp:
            content = fp.read().strip()
        if content == "stop":
            return True
        return False
    except:
        return True


def timing(expression, interval=10, logger=logging):
    def decorator(func):
        @functools.wraps(func)
        def implement(*args, **kwargs):
            def run():
                name = re.search('(exec.*?)\s+', str(func)).group(1)
                lasttime = int(time.time())
                set_survival(name, isstop=True)

                logger.info("进程{0}，<{1}>。开始运行了。".format(name, expression))
                while 1:
                    if expression == "" or check_timing(name, expression, start_time=time.time()):
                        logger.info("进程<{0}>，<{1}>。到点啦，开始执行任务。".format(name, expression))
                        set_survival(name, isstop=False)
                        try:
                            func(*args, **kwargs)
                            logger.info("进程{0}，<{1}>。执行成功了。".format(name, expression))
                        except Exception as error:
                            logger.error("进程{0}，<{1}>。执行失败了。{2}".format(name, expression, error))
                        set_survival(name, isstop=True)

                    time.sleep(interval)

                    if int(time.time()) - lasttime >= 30:
                        lasttime = int(time.time())
                        logger.info("进程<{0}>，<{1}>。运行正常。等待启动时间。".format(name, expression))

            t = threading.Thread(target=run)
            t.setDaemon(True)
            t.start()

        return implement
    return decorator


if __name__ == "__main__":
    start_time = None
    # 目前cron表达式中只支持指定整点功能 只有以下两种数据格式可用
    cron_info = "0 0 13 * * ?"              # 每天13点
    cron_info = "0 0 10,14,16 * * ?"        # 每天10点，14点，16点
    cron_info = "0 0 17 * * ?"        # 每天17点

    start_time = time.time()
    # start_time = time.mktime(time.strptime("2016-08-02 11:23:42", "%Y-%m-%d %H:%M:%S" ))
    # start_time = time.mktime(time.strptime("2016-09-25 15:00:00", "%Y-%m-%d %H:%M:%S"))
    # cron_info = "0 0 1/2 * * ? "            # 从start_time开始 1小时后 每2小时执行一次
    # cron_info = "0 0 0/2 * * ? "            # 从start_time开始 每2小时执行一次
    cron_info = "0 0 0/1 * * ? "            # 从start_time开始 每1小时执行一次
    # cron_info = "0 0/10 * * * ? "            # 从start_time开始 每1小时执行一次

    # start_time = 0
    # cron_info = "0 0 15,16 16 * ?"          # 每月16号的 15点、16点
    # cron_info = "0 0 18 1,3,5,7,9,11 * ?"          # 每月1,3,5,7,9,11号的 17点

    # cron_info = "0 57 0/1 * * ?"          # 从start_time开始 每小时的30分执行
    # cron_info = "0 57 0/1 * * ?"

    print(check_timing("test", cron_info, start_time=start_time))
    print(cron_info)



