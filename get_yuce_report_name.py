from lxml import etree
import pandas as pd


def get_local_html_data(path: str):
    fp = open(path,'rb')
    html = fp.read().decode('utf-8')
    selector = etree.HTML(html)
    # strs = selector.xpath("//ul[@class='list_ul']//li/descendant::text()")
    datas = selector.xpath("//ul[@class='list_ul']//li")
    
    report_list = []
    for i in datas:
        msg_list = []
        msg_list.append(i.xpath("./h5/descendant::text()")[0])
        msg_list.append(i.xpath("./h4/descendant::text()")[0])
        msg_list.append(i.xpath("./p/descendant::text()")[0])
        report_list.append(msg_list)
    return report_list


if __name__ == '__main__':
    local_html_path0 = r'C:\Users\WIN7\Desktop\下载文件\日报\20200511\yuce0.html'
    report_list0 = get_local_html_data(local_html_path0)

    local_html_path1 = r'C:\Users\WIN7\Desktop\下载文件\日报\20200511\yuce1.html'
    report_list1 = get_local_html_data(local_html_path1)

    local_html_path2 = r'C:\Users\WIN7\Desktop\下载文件\日报\20200511\yuce2.html'
    report_list2 = get_local_html_data(local_html_path2)

    report_list = report_list0 + report_list1 + report_list2
    report_df = pd.DataFrame(report_list, columns=['名称', '创建人', '说明'])
    report_df.to_excel(r'C:\Users\WIN7\Desktop\下载文件\日报\20200511\御策快速报表清单.xlsx')