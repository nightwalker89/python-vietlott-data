import os
from bs4 import BeautifulSoup
import requests
import re
import csv
import json
from concurrent.futures import ThreadPoolExecutor as PoolExecutor



# Get current data
def get_current_keno_id() -> int:
    url = "https://vietlott.vn/vi/trung-thuong/ket-qua-trung-thuong/winning-number-keno"
    html = requests.get(url).text
    match = re.findall(r'view-detail-keno-result\?id=([0-9]+)"', html)
    if match:
        return int(match[0])
    return 0


# Format draw id, padding zeros
def format_keno_id(draw_id): 
    return "{:0>7d}".format(draw_id)


# Get number from draw id
def get_keno_result_page(draw_id):
    if int(draw_id) == draw_id:
        draw_id = format_keno_id(draw_id)
    url = "https://vietlott.vn/vi/trung-thuong/ket-qua-trung-thuong/view-detail-keno-result?id=" + draw_id
    return requests.get(url).text


# Parse Keno Result info
def parse_keno_result(html_text, draw_id):
    html = html_text

    # Missing keno draw
    missing_text = 'Không tìm thấy kết quả'
    if missing_text in html_text:
        return {
        'Id' : format_keno_id(draw_id),
        'Date' : 'missing',
        'Balls' : 'missing',
        'Wins' : 'null'
    }

    soup = BeautifulSoup(html, 'html.parser')

    # get balls
    balls = re.findall(r'<span class="bong_tron small">([0-9]{2})\<\/span>', html)
    if len(balls) != 20:
        print("Cannot get balls", balls)
        return None
    
    # get date
    match = re.findall(r'<td(.*)>([0-9]+\/[0-9]+\/[0-9]+)<\/', html)
    if len(match) == 0 or len(match[0]) == 0:
        print("Không thể lấy ngày quay")
        return None
    draw_date = match[0][1]
    print(format_keno_id(draw_id), draw_date)

    # get winning
    content_div = soup.find_all("div", class_="tab-content")
    if len(content_div) <= 0:
        print("# Không lấy được bảng kết quả")
        return None
    content_div = content_div[0]
    tabs = content_div.find_all("div", class_="tab-pane")
    if len(tabs) != 12:
        print("# Bảng kết quả có thay đổi")
        return None    

     
    all_wins = []
    for tab in tabs:
        rows = tab.select("tr.tr0, tr.tr1")
        wins = []
        for row in rows:
            win_cell = row.select("td")[1]
            match = re.findall(r': ([0-9]+)', win_cell.text)
            if len(match) > 0:
                wins.append(int(match[0]))
            else:
                match = re.findall(r' \(([0-9]+)\)', win_cell.text)                    
                if len(match) > 0:
                    wins.append(int(match[0]))
                else:    
                    wins.append(0)
        all_wins.append(wins)
    if len(all_wins) == 0:
        print("# không thể lấy danh sách giải thưởng")
        return None
    
    return {
        'Id' : format_keno_id(draw_id),
        'Date' : draw_date,
        'Balls' : balls,
        'Wins' :all_wins
    }


def get_keno_result(draw_id):
    html = get_keno_result_page(draw_id)
    return parse_keno_result(html,draw_id)

# print(soup)

# latest_id = get_current_keno_id()
# latest_id = format_keno_id(1)
# info = get_keno_result(latest_id)

# print(latest_id)
# print(info)
#############################################
# MAIN
#############################################
results = []
path = os.getcwd()

# data folder 
data_path = os.path.join(path, 'data')
if not os.path.exists(data_path):
    os.makedirs(data_path)

# data file    
result_file = os.path.join(data_path,"results.csv")
wins_file   = os.path.join(data_path,"wins.csv")

if not os.path.exists(result_file):
    with open(result_file, 'w'): pass
if not os.path.exists(wins_file):
    with open(wins_file, 'w'): pass

# Read local draw id
csv_delimiter = "|"
local_id = 0
with open(result_file,'r', newline='') as file:
    reader = csv.reader(file, delimiter=csv_delimiter)
    rows = list(reader)
    
    if len(rows) > 0:
        if rows[-1]: 
            local_id = int(rows[-1][0])
        elif rows[-2]:
            local_id = int(rows[-2][0])
print("# Local ID: #", local_id)

# Get current draws id
online_id = get_current_keno_id()
if online_id == 0:
    print("# Không thể lấy kết quả mới nhất")
    exit()
print("# Online ID: #", online_id)

if local_id == online_id:
    print("# Dữ liệu up-to-date không cần cập nhật")
    exit()

#### config request batch size ####
batch_size = 100
threads    = 20
#start      = local_id
#end        = online_id

# create a thread pool of 4 threads
if __name__ == "__main__":

    def append_to_csv(csv_file, rows):
            with open(csv_file, 'a', newline='') as file:
                writer = csv.writer(file, delimiter=csv_delimiter)
                writer.writerows(rows)

    def sortFirst(val): 
        return val['Id']

    def get_keno(draw_id):
        global results
        r = get_keno_result(draw_id)
        results.append(r)
    
    batchs = range(local_id + 1, online_id, batch_size)
    if online_id - local_id == 1: 
        batchs = [local_id+1]
        batch_size = 1
    for i in batchs:
        start  = i
        end    = i + batch_size
        if end >= online_id:
            end = online_id + 1
        draws_id = [*range(start,end)]
        
        with PoolExecutor(max_workers=threads) as executor:

            # distribute the 1000 URLs among 4 threads in the pool
            # _ is the body of each page that I'm ignoring right now
            for _ in executor.map(get_keno, draws_id):
                pass

            # write row to csv
            results.sort(key=sortFirst) 
            _result_rows = []
            _win_rows    = []
            for line in results:
                print(line)
                if line['Date'] != 'missing': 
                    _result_rows.append([line['Id'], line['Date'], ",".join(line['Balls']) ])
                    
                    _win_rows.append([line['Id'], line['Date'], line['Wins'] ])
                    print(_result_rows)
                else:
                    print(line['Id'] + ' is missing')
                    _result_rows.append([line['Id'], 'missing', 'missing' ])
                    
                    _win_rows.append([line['Id'], 'missing', 'null' ])
            
            print("Add new rows:", len(_result_rows))
            append_to_csv(result_file, _result_rows)
            append_to_csv(wins_file, _win_rows)                      
            results = []

    results.sort(key=sortFirst)        
    #print(results)
    print("Done")
    
    #from libs import keno
    #keno.test("Cuong")