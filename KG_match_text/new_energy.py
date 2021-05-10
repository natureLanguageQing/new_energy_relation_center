# 探索公司二级关系中的公司pid
import json
import os
import pandas as pd

from json import JSONDecodeError

# 知识图谱地址
knowledge_path = "../company_knowledge_base/company_detail_company"
# 新闻地址
news_path = "../chinese_news_double"
# 二级相关企业pid保存地址
second_pid_path = "second_pid.csv"

news_path_list = os.listdir(news_path)
knowledge_path_list = os.listdir(knowledge_path)
print(len(knowledge_path_list))
second_pid = open(second_pid_path, "w", encoding="utf-8")
for knowledge_path_one in knowledge_path_list:
    try:
        knowledge_json = json.load(open(os.path.join(knowledge_path, knowledge_path_one), "r", encoding="utf8"))
    except JSONDecodeError:
        # 因为爬虫的时候出现的问题导致企业知识图谱中可能存在一些问题
        print("JSONDecodeError", os.path.join(knowledge_path, knowledge_path_one))
        continue
    for total_list in knowledge_json['data'].values():
        if "pid" in total_list:
            print(total_list["pid"])
            second_pid.writelines(total_list["pid"] + "\n")
        if "list" in total_list:
            for second_total in total_list["list"]:
                if "pid" in second_total:
                    try:
                        if int(second_total["pid"]):
                            second_pid.writelines(second_total["pid"] + "\n")
                    except ValueError:
                        continue

pd.read_csv(second_pid_path).drop_duplicates().to_csv(second_pid_path, index=False)
