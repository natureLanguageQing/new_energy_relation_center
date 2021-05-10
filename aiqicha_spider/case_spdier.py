import xlrd
import os

export_txt_path = "../company_data_txt/"
if not os.path.exists(export_txt_path):
    os.mkdir(export_txt_path)

company_data = xlrd.open_workbook("../企业爱企查导出数据/company_data/【爱企查】-新能源企业.xls")
export_txt_file = open(os.path.join(export_txt_path, "【爱企查】-新能源企业.txt"), "w", encoding="utf8")
company_data = company_data.sheet_by_index(0)
for company_one in range(3, company_data.nrows):
    link = company_data.hyperlink_map.get((company_one, 0))
    url = "" if link is None else link.url_or_path
    url = url.strip("https://aiqicha.baidu.com/company_detail_")
    url = url.strip("?fr=excel")
    if url != "":
        export_txt_file.writelines(url + "\n")
