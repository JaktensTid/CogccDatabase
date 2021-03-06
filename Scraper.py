from lxml import html
from lxml import etree
from datetime import date
import logging
import requests
import requests.exceptions
import socket
sceleton = 'http://cogcc.state.co.us/cogis/ProductionWellMonthly.asp?APICounty=%s&APISeq=%s&APIWB=%s&Year=%s'

def get_rows_by_link(link, recursive_count=0):
    logging.info('Downloading ' + link)
    response = ""
    try:
        response = requests.get(link, timeout=10)
    except requests.exceptions.RequestException:
        if recursive_count > 3:
            return ()
        return get_rows_by_link(link,recursive_count=recursive_count + 1)
    document = html.fromstring(response.content)
    second_table = ""
    try:
        second_table = document.xpath("//table")[1]
    except IndexError:
        return ()
    rows = []
    for tr in second_table.xpath("//tr[position()>5]"):
        row = {}
        row["month"] = tr.xpath("./td[1]//text()")[0]
        row["formation"] = tr.xpath("./td[2]//text()")[0]
        row["well_status"] = tr.xpath("./td[4]//text()")[0]
        row["days_prod"] = tr.xpath("./td[5]//text()")[0]
        row["bom"] = tr.xpath("./td[7]//text()")[0]
        row["produced"] = tr.xpath("./td[8]//text()")[0]
        row["oil_sold"] = tr.xpath("./td[9]//text()")[0]
        row["adjusted"] = tr.xpath("./td[10]//text()")[0]
        row["eom"] = tr.xpath("./td[11]//text()")[0]
        row["gravity"] = tr.xpath("./td[12]//text()")[0]
        row["prod"] = tr.xpath("./td[7]//text()")[-1]
        row["flared"] = tr.xpath("./td[8]//text()")[-1]
        row["used"] = tr.xpath("./td[9]//text()")[-1]
        row["shrinkage"] = tr.xpath("./td[10]//text()")[-1]
        row["gas_sold"] = tr.xpath("./td[11]//text()")[-1]
        row["btu"] = tr.xpath("./td[12]//text()")[-1]
        row["gas_tbg"] = tr.xpath("./td[14]//text()")[-1]
        row["gas_csg"] = tr.xpath("./td[15]//text()")[-1]
        row["water_prob"] = tr.xpath("./td[13]//text()")[0]
        row["water_disp"] = tr.xpath("./td[13]//text()")[-1]
        row["water_tbg"] = tr.xpath("./td[14]//text()")[0]
        row["water_csg"] = tr.xpath("./td[15]//text()")[0]
        for key in row.keys():
            value = row[key]
            row[key] = value.strip() if not value.isspace() else None
        rows.append(row)
    return tuple(rows)

def download_data_by_well_one_year(api_county_code, api_seq_num, sidetrack_num, year):
    link = sceleton % (api_county_code, api_seq_num, sidetrack_num, year)
    return get_rows_by_link(link)

def download_all_data_by_well(api_county_code, api_seq_num, sidetrack_num):
    data_for_years = []
    for year in range(1999, date.today().year + 1):
        data_for_years.append(download_data_by_well_one_year(api_county_code,api_seq_num,sidetrack_num, year))
    return data_for_years
