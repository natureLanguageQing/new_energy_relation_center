# https://aiqicha.baidu.com/relations/relationalMapAjax?pid=78817378272371&_=1620444910043
import json
from multiprocessing import Pool

import requests
from fake_useragent import UserAgent
import pandas as pd
ua = UserAgent()


def doSpider():
    c_name = pd.read_csv("c_data/c.csv").values.tolist()
    for c_one in c_name:
        url = "http://api.mjj666.cn/search/?term=" + c_one[0] + "&offset=0&size=25&access_token=&openid="
        entity_json_list = []
        headers = {'User-Agent': ua.random}
        data = []
        for i in range(200):
            try:
                entity_json = requests.get(url, headers=headers)
                entity_json_load = json.loads(entity_json.text)
                if entity_json_load not in entity_json_list:
                    entity_json_list.append(entity_json_load)
                    graph = open("news_data/" + c_one[0] + ".learn.graph.json", "a", encoding="utf-8")
                    data.extend(entity_json_load['list'])
                    pd.DataFrame(data).drop_duplicates().to_csv("news_csv/海天味业." + c_one[0] + ".graph.csv",
                                                                encoding="utf-8")
                    entity_json_load = json.dumps(entity_json_load, ensure_ascii=False)
                    graph.write(entity_json_load + "\n")
            except:
                continue


if __name__ == "__main__":

    ps = Pool(16)
    for company_one in range(10000000000):
        ps.apply_async(doSpider)  # 异步执行
    # 关闭进程池，停止接受其它进程
    ps.close()
    # 阻塞进程
    ps.join()
