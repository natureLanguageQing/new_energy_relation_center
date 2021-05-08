# -*- coding: utf-8 -*-

import requests

from datetime import datetime, timedelta

from lxml import etree

import csv

import os

from time import sleep
from random import randint


def parseTime(unformatedTime):
    if '分钟' in unformatedTime:
        minute = unformatedTime[:unformatedTime.find('分钟')]
        minute = timedelta(minutes=int(minute))
        return (datetime.now() -
                minute).strftime('%Y-%m-%d %H:%M')
    elif '小时' in unformatedTime:
        hour = unformatedTime[:unformatedTime.find('小时')]
        hour = timedelta(hours=int(hour))
        return (datetime.now() -
                hour).strftime('%Y-%m-%d %H:%M')
    else:
        return unformatedTime


def dealHtml(html):
    results = html.xpath('//div[@class="result-op c-container xpath-log new-pmd"]')

    saveData = []

    for result in results:
        title = result.xpath('.//h3/a')[0]
        title = title.xpath('string(.)').strip()

        summary = result.xpath('.//span[@class="c-font-normal c-color-text"]')[0]
        summary = summary.xpath('string(.)').strip()

        # ./ 是直接下级，.// 是直接/间接下级
        infos = result.xpath('.//div[@class="news-source"]')[0]
        try:
            source, dateTime = infos.xpath(".//span[last()-1]/text()")[0], \
                               infos.xpath(".//span[last()]/text()")[0]
        except IndexError:
            return
        dateTime = parseTime(dateTime)

        print('标题', title)
        print('来源', source)
        print('时间', dateTime)
        print('概要', summary)
        print('\n')

        saveData.append({
            'title': title,
            'source': source,
            'time': dateTime,
            'summary': summary
        })
    with open(os.path.join("../baidu_新能源企业_news_double", fileName), 'a+', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        for row in saveData:
            writer.writerow([row['title'], row['source'], row['time'], row['summary']])


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'Referer': 'https://www.baidu.com/s?rtt=1&bsst=1&cl=2&tn=news&word=%B0%D9%B6%C8%D0%C2%CE%C5&fr=zhidao'
}

url = 'https://www.baidu.com/s'

params = {
    'ie': 'utf-8',
    'medium': 0,
    # rtt=4 按时间排序 rtt=1 按焦点排序
    'rtt': 1,
    'bsst': 1,
    'rsv_dl': 'news_t_sk',
    'cl': 2,
    'tn': 'news',
    'rsv_bp': 1,
    'oq': '',
    'rsv_btype': 't',
    'f': 8,
}


def doSpider(keyword, sortBy='focus'):
    '''
    :param keyword: 搜索关键词
    :param sortBy: 排序规则，可选：focus(按焦点排序），time(按时间排序），默认 focus
    :return:
    '''
    global fileName
    fileName = '{}.csv'.format(keyword)
    if not os.path.exists(os.path.join("../baidu_新能源企业_news_double")):
        os.mkdir(os.path.join("../baidu_新能源企业_news_double"))
    if not os.path.exists(fileName):
        with open(os.path.join("../baidu_新能源企业_news_double", fileName), 'w+', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['title', 'source', 'time', 'summary'])

    params['wd'] = keyword
    if sortBy == 'time':
        params['rtt'] = 4

    response = requests.get(url=url, params=params, headers=headers)

    html = etree.HTML(response.text)

    dealHtml(html)

    total = html.xpath('//div[@id="header_top_bar"]/span/text()')[0]

    total = total.replace(',', '')

    try:
        total = int(total[7:-1])
    except:
        return
    pageNum = total // 10
    if pageNum > 100:
        pageNum = 100
    for page in range(1, pageNum):
        print('第 {} 页\n\n'.format(page))
        headers['Referer'] = response.url
        params['pn'] = page * 10

        response = requests.get(url=url, headers=headers, params=params)

        html = etree.HTML(response.text)

        dealHtml(html)

        sleep(randint(2, 4))
    ...


if __name__ == "__main__":

    import pandas as pd

    law_entity = pd.read_excel("../company_data/【爱企查】-新能源企业.xls").values.tolist()
    for law_entity_one in law_entity:
        law_query_one = law_entity_one[0].strip("\n")
        law_query_two = law_entity_one[1].strip("\n")
        fileName = '{}.csv'.format(law_query_one + " " + law_query_two)

        if not os.path.exists(os.path.join("../baidu_新能源企业_news_double", fileName)):
            doSpider(keyword=law_query_one + " " + law_query_two, sortBy='focus')
