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
    headers = {'User-Agent': ua.random}
    entity_json = requests.get(url, headers=headers)
    entity_json_load = json.loads(entity_json.text)
    graph = open("../news_data/" + str(index) + ".learn.graph.json", "a", encoding="utf-8")
    entity_json_load = json.dumps(entity_json_load, ensure_ascii=False)
    graph.write(entity_json_load + "\n")
    sleep(10)

if __name__ == "__main__":
    import xlrd

    ps = Pool(2)

    company_data = xlrd.open_workbook("../company_data/【爱企查】-科技有限责任公司.xls")
    company_data = company_data.sheet_by_index(0)
    print(company_data)

    for company_one in range(5000):
        link = company_data.hyperlink_map.get((company_one, 0))
        url = "" if link is None else link.url_or_path
        url = url.strip("https://aiqicha.baidu.com/company_detail_")
        url = url.strip("?fr=excel")
        print(url)
        ps.apply_async(doSpider, args=(url,))  # 异步执行
    # # 关闭进程池，停止接受其它进程
    ps.close()
    # # 阻塞进程
    ps.join()
