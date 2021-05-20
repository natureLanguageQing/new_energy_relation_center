# https://aiqicha.baidu.com/relations/relationalMapAjax?pid=78817378272371&_=1620444910043
# 根据二级pid找到企业信息
import json
from multiprocessing import Pool
from time import sleep, time

import requests
from fake_useragent import UserAgent
import os

ua = UserAgent()
export_txt_path = "../company_knowledge_base/company_data_second_pid/"
if not os.path.exists(export_txt_path):
    os.mkdir(export_txt_path)


def doSpider(index):
    print(str(time()))
    url = "https://aiqicha.baidu.com/relations/relationalMapAjax?pid=" + str(
        index) + "&_=" + str(time())
    print(url)
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36",
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br'}
    entity_json = requests.get(url, headers=headers)
    print(entity_json.text)
    try:
        entity_json_load = json.loads(entity_json.text)
    except:
        return
    graph = open(os.path.join(export_txt_path, str(index) + ".learn.graph.json"), "w", encoding="utf-8")
    entity_json = json.dumps(entity_json_load, ensure_ascii=False, indent=2)
    graph.write(entity_json)
    sleep(10)


if __name__ == "__main__":
    import os

    pid_path = '../company_data_txt'

    export_txt_file = open(os.path.join(pid_path, "company_detail_company_second_pid.csv"), "r", encoding="utf8")
    for company_one in list(set(export_txt_file.readlines())):
        # ps.apply_async(doSpider, args=(company_one.strip("\n")))  # 异步执行
        if not os.path.exists(os.path.join(export_txt_path, str(company_one.strip("\n")) + ".learn.graph.json")):
            doSpider(company_one.strip("\n"))  # 异步执行
