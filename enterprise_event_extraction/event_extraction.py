import pandas as pd
import os

from pandas.errors import EmptyDataError

graph_news_relation_base = "../graph_新能源企业_news_relation"
graph_news_relation_data_set = []
# 获取新闻数据
graph_news_relation = os.listdir(graph_news_relation_base)
# 获取三元组数据
law_entity = pd.read_csv("../company_base_triple/company_data_新能源企业_triple.csv").drop_duplicates().values.tolist()
for graph_news_relation_one in graph_news_relation:
    try:
        graph_news_relation_data = pd.read_csv(
            os.path.join(graph_news_relation_base, graph_news_relation_one)).values.tolist()
    except EmptyDataError:
        continue
    for graph_news_relation_data in graph_news_relation_data:
        # print(graph_news_relation_data)
        graph_news_relation_data_set.extend(graph_news_relation_data)
graph_news_relation_data_set = list(set(graph_news_relation_data_set))
pd.DataFrame(graph_news_relation_data_set).drop_duplicates().to_csv("graph_news_relation_data_set.csv", index=False)

graph_news_relation_data_export = []
for i in law_entity:
    for news_relation in graph_news_relation_data_set:
        if isinstance(i[0], str) and isinstance(i[2], str) and isinstance(news_relation, str):
            news_relation_part = news_relation.strip(i[0])
            if i[0] in news_relation and i[2] in news_relation_part:
                # print(i, news_relation)
                graph_news_relation_data_export.append([i[0], i[1], i[2], news_relation])
pd.DataFrame(graph_news_relation_data_export).drop_duplicates().to_csv("event_extraction.csv", index=False)
pd.DataFrame(graph_news_relation_data_export).drop_duplicates().to_excel("event_extraction.xlsx", index=False)
