# https://aiqicha.baidu.com/relations/relationalMapAjax?pid=78817378272371&_=1620444910043
import json
from multiprocessing import Pool
from time import sleep
from random import randint
import requests


def doSpider(index, export_file_path):
    """

    :param export_file_path: 数据导出文件夹地址
    :param index: 爱企查 pid
    :return:
    """
    url = "https://aiqicha.baidu.com/relations/relationalMapAjax?pid=" + str(
        index) + "&_=1620444910043"
    print(url)
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36"}
    entity_json = requests.get(url, headers=headers)
    print(entity_json.text)
    entity_json_load = json.loads(entity_json.text)
    graph = open(os.path.join(export_file_path, str(index) + ".json"), "a", encoding="utf-8")
    json.dump(entity_json_load, graph, ensure_ascii=False, indent=3)
    sleep(randint(2, 4))


if __name__ == "__main__":
    import xlrd
    import os

    ps = Pool(2)
    base_path = "../company_data_2021年5月8日"
    export_path = "company_detail"
    if not os.path.exists(export_path):
        os.mkdir(export_path)
    for one_path in os.listdir(base_path):
        company_data = xlrd.open_workbook(os.path.join(base_path, one_path))
        company_data = company_data.sheet_by_index(0)
        for company_one in range(5000):
            link = company_data.hyperlink_map.get((company_one, 0))
            url = "" if link is None else link.url_or_path
            url = url.strip("https://aiqicha.baidu.com/company_detail_")
            url = url.strip("?fr=excel")
            if url != "":
                ps.apply_async(doSpider, args=(url, export_path))  # 异步执行

    # # 关闭进程池，停止接受其它进程
    ps.close()
    # # 阻塞进程
    ps.join()
