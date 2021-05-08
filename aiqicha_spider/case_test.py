# https://aiqicha.baidu.com/relations/relationalMapAjax?pid=78817378272371&_=1620444910043
import json
from multiprocessing import Pool
from time import sleep

import requests
from fake_useragent import UserAgent

ua = UserAgent()


def doSpider(index):
    url = "https://aiqicha.baidu.com/relations/relationalMapAjax?pid=" + str(
        index) + "&_=1620444910043"
    print(url)
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36"}
    entity_json = requests.get(url, headers=headers)
    print(entity_json.text)
    entity_json_load = json.loads(entity_json.text)
    graph = open("../company_detail_company/" + str(index) + ".learn.graph.json", "a", encoding="utf-8")
    entity_json_load = json.dumps(entity_json_load, ensure_ascii=False)
    graph.write(entity_json_load + "\n")
    sleep(10)


if __name__ == "__main__":
    import xlrd
    import os

    export_txt_path = "../company_data_txt/"
    if not os.path.exists(export_txt_path):
        os.mkdir(export_txt_path)
    ps = Pool(2)

    company_data = xlrd.open_workbook("../company_data_2021年5月8日/【爱企查】-科技有限责任公司.xls")
    export_txt_file = open(os.path.join(export_txt_path, "【爱企查】-科技有限责任公司.txt"), "w", encoding="utf8")
    company_data = company_data.sheet_by_index(0)
    print(company_data)
    for company_one in range(3,company_data.nrows):
        link = company_data.hyperlink_map.get((company_one, 0))
        url = "" if link is None else link.url_or_path
        print(url)
        url = url.strip("https://aiqicha.baidu.com/company_detail_")
        url = url.strip("?fr=excel")
        if url != "":
            print(url)
            export_txt_file.writelines(url+"\n")
            # ps.apply_async(doSpider, args=(url,))  # 异步执行
    # # 关闭进程池，停止接受其它进程
    ps.close()
    # # 阻塞进程
    ps.join()
