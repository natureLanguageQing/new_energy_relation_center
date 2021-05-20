# 构建公司三元组
# 基于一维关系 二维关系构建三元组
import csv
import json
import os
import pandas as pd

from json import JSONDecodeError

# 知识图谱地址
knowledge_path = "../company_knowledge_base/news_data"
# 新闻地址
news_path = "../chinese_news_double"
# 企业三元组保存地址 实体 实体 关系
second_pid_path = "../company_base_triple/news_data_triple.csv"

news_path_list = os.listdir(news_path)
knowledge_path_list = os.listdir(knowledge_path)
print(len(knowledge_path_list))
second_pid = open(second_pid_path, "w", encoding="utf-8")
triple_writer = csv.writer(second_pid, dialect='excel')
triple_writer.writerow(["实体", "关系", "实体"])

for knowledge_path_one in knowledge_path_list:
    try:
        knowledge_json = json.load(open(os.path.join(knowledge_path, knowledge_path_one), "r", encoding="utf8"))
    except JSONDecodeError:
        # 因为爬虫的时候出现的问题导致企业知识图谱中可能存在一些问题
        print("JSONDecodeError", os.path.join(knowledge_path, knowledge_path_one))
        continue
    # 公司完整名称
    company_ent_name = ""
    try:
        knowledge_json['data'].items()
    except:
        continue
    for total_key, total_list in knowledge_json['data'].items():
        if "entName" in total_list:
            company_ent_name = total_list["entName"]
        if "legalPerson" in total_list:
            # 法人关系三元组
            triple_writer.writerow([company_ent_name, "legalPerson", total_list["legalPerson"]])
            # 简称关系三元组
            triple_writer.writerow([company_ent_name, "logoWord", total_list["logoWord"]])
        if "list" in total_list:
            for second_total in total_list["list"]:
                for second_title, second_value in second_total.items():
                    if second_value and isinstance(second_value, str) and second_value not in {"-"} and len(
                            second_value) > 1:
                        if total_key + "_" + second_title not in ["directorsData_personId", "directorsData_personLink",
                                                                  "holdsData_pid", "investRecordData_pid",
                                                                  "investRecordData_logo",
                                                                  "shareholdersData_personLink",
                                                                  "shareholdersData_personId", "shareholdersData_logo",
                                                                  "holdsData_logo", "shareholdersData_pid",
                                                                  "branchsData_pid", "branchsData_logo",
                                                                  "directorsData_personLogo",
                                                                  "shareholdersData_personLogo"]:
                            second_value.strip("\n")
                            triple_writer.writerow([company_ent_name, total_key + "_" + second_title, second_value])
print(len(second_pid_path))
pd.read_csv(second_pid_path).drop_duplicates().to_csv(second_pid_path, index=False)
pd.read_csv(second_pid_path).drop_duplicates().to_excel("../company_base_triple/news_data_triple.xlsx", index=False)
