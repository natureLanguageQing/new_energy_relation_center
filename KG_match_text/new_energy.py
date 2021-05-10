# 探索公司二级关系中的公司pid
import json
import os

# 知识图谱地址
from json import JSONDecodeError

knowledge_path = "../company_knowledge_base/company_detail_company"
# 新闻地址
news_path = "../chinese_news_double"

news_path_list = os.listdir(news_path)
knowledge_path_list = os.listdir(knowledge_path)
print(len(knowledge_path_list))
second_pid = open("second_pid.csv", "w", encoding="utf-8")
for knowledge_path_one in knowledge_path_list:
    try:
        knowledge_json = json.load(open(os.path.join(knowledge_path, knowledge_path_one), "r", encoding="utf8"))
    except JSONDecodeError:
        print("JSONDecodeError", os.path.join(knowledge_path, knowledge_path_one))
        continue
    for total_list in knowledge_json['data'].values():
        print(total_list)
        if "pid" in total_list:
            print(total_list["pid"])
            second_pid.writelines(total_list["pid"] + "\n")
        if "list" in total_list:
            for second_total in total_list["list"]:
                if "pid" in second_total:
                    print(second_total["pid"])
                    try:
                        if int(second_total["pid"]):
                            second_pid.writelines(second_total["pid"] + "\n")
                    except ValueError:
                        continue
import pandas as pd

pd.read_csv("second_pid.csv").drop_duplicates().to_csv("second_pid.csv", index=False)
