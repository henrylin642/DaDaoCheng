import pytz
from datetime import datetime, timedelta, date
import pandas as pd
import os
import json
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ga_api.json'

#%% 設定日期範圍
def get_date_data():
    # 設定時區為台灣時間
    taipei_timezone = pytz.timezone('Asia/Taipei')
    datetime_taipei = datetime.now(taipei_timezone).date()
    today = datetime_taipei
    yesterday = today - timedelta(days=1)
    this_week_start = today - timedelta(days=today.weekday())
    this_week_end = this_week_start + timedelta(days=6)
    last_week_start = this_week_start - timedelta(days=7)
    last_week_end = last_week_start + timedelta(days=6)
    this_month_start = date(today.year, today.month, 1)
    this_month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = date(last_month_end.year, last_month_end.month, 1)
    return today,yesterday,this_week_start,this_week_end,last_week_start,last_week_end,this_month_start,this_month_end,last_month_start,last_month_end

def upload(df,selected_db):
    filename =  "data/" + df[df['db'] == selected_db]['filename'].values[0]
    df_origin = pd.read_csv(filename, encoding="utf-8-sig")
    return df_origin

def arrange_scan_data(df_light,df_coor,df_arobjs):
    # 匯入掃描數據 Timestamp,lig_id,Tenant ID,SDK Instance ID,Decoder ID
    df_scan = pd.read_csv("data/scandata.csv",encoding="utf=8-sig",usecols=['Timestamp','lig_id'])
    df_scan = df_scan.rename(columns={'Timestamp':'scantime'})
    # 剔除不合理數據
    df_scan = df_scan[df_scan['lig_id'] >=100]
    df_scan = df_scan[df_scan['lig_id'] <=10000]
    df_scan['scantime'] = pd.to_datetime(df_scan['scantime'])
    df_scan_lastestday = df_scan['scantime'].max()

    # 匯入light_id數據 Id,Name,Name [Coordinate systems]
    #df_light = pd.read_csv("data/light_2023-06-26_11h36m20.csv",encoding="utf=8-sig",usecols=['Id','Name [Coordinate systems]'])
    df_light = df_light.rename(columns={'Id':'lig_id','Name [Coordinate systems]':'coor_name'})
    df_light = df_light.dropna(subset=['coor_name'])
    df_scan_coor = df_scan.merge(df_light, how = 'left' , on = 'lig_id')

    # 匯入坐標系數據 Id, Name, Created at,  Name[Scenes], Created at[Scenes]
    #df_coor = pd.read_csv("data/coordinate_system_2023-07-06_23h22m11.csv",encoding="utf=8-sig",usecols=['Id','Name','Created at','Name [Scenes]'])
    df_coor = df_coor.rename(columns={'Id':'coor_id','Name':'coor_name','Created at':'coor_createdtime','Name [Scenes]':'scene_name'})
    df_coor['coor_createdtime'] = pd.to_datetime(df_coor['coor_createdtime'], format='%Y年%m月%d日 %H:%M')
    df_coor_lastestday = df_coor['coor_createdtime'].max()
    df_scan_coor_scene = df_scan_coor.merge(df_coor, how = 'left' , on = 'coor_name')

    # 匯入坐標系城市數據
    df_coor_city = pd.read_csv("data/coor_city.csv",encoding="utf=8-sig")
    df_scan_coor_scene_city = df_scan_coor_scene.merge(df_coor_city, how = 'left' , on = 'coor_name')
    df_scan_coor_scene_city = df_scan_coor_scene_city.dropna(subset=['coor_name'])
    df_scan_coor_scene_city.to_csv('data/掃描data.csv', encoding='utf-8-sig', index = False )
    df_arobjs = df_arobjs.rename(columns={"Id":"obj_id","Name":"obj_name","Name [Scene]":"obj_scene"})
    return df_scan_coor_scene_city,df_coor_city,df_coor,df_arobjs

def H24hour_scans(df,day,coors):
    df_filter = df[df['scantime'].dt.date==day]
    
    # 创建表格数据
    table_data = {'小時': []}
    for coor in coors:
        table_data[coor] = []

    # 填入表格数据
    for i in range(24):
        # hour_str = f"{i:02d}:00"
        table_data['小時'].append(i)
        # 根据日期和小时筛选数据
        filtered_data = df_filter[df_filter['scantime'].dt.hour == i]

        # 根据区域进行分组计数
        scans = filtered_data.groupby('coor_name').size()

        # 填入表格数据
        for coor in coors:
            count = scans.get(coor, 0)
            table_data[coor].append(count)

    # 建立最终表格
    table = pd.DataFrame(table_data).set_index('小時')

    return table,df_filter

def csv_download(df):
    csv_download = df.to_csv().encode("utf-8-sig")
    return csv_download


def get_scenes(df,select_coors): #df_coor
    scenes_list = []
    for i in range(len(select_coors)):
        scenes_list.extend(df[df['coor_name'].isin(select_coors)]['scene_name'].iloc[i].split(","))
    scenes_string = ' '.join(scenes_list)
    return scenes_list


def get_GA_data(df_arobjs,start_date,end_date,scenes):
    date_range = {
    'start_date': start_date.strftime('%Y-%m-%d'),
    'end_date': end_date.strftime('%Y-%m-%d')   
    }
    
    def vlookup(key, df, column, return_column):
        try:
            return df.loc[df[column] == key, return_column].iloc[0]
        except IndexError:
            return None

    client = BetaAnalyticsDataClient()
    property_id='270740329'

    request = RunReportRequest(property=f"properties/{property_id}")
    request.date_ranges.append(date_range)
    request.dimensions.append({'name': 'customEvent:ID'})
    request.metrics.append({'name': 'eventCount'})

    response = client.run_report(request)

    obj_id_lst =[]
    obj_name_lst=[]
    obj_scene_lst=[]
    click_count_lst = []

    for row in response.rows:
        obj_id = row.dimension_values[0].value
        click_count = row.metric_values[0].value
        if obj_id and obj_id.isdigit():
            obj_id = int(obj_id)
            obj_name = vlookup(obj_id, df_arobjs, "obj_id", "obj_name")
            obj_scene = vlookup(obj_id, df_arobjs, "obj_id", "obj_scene")
            obj_id_lst.append(obj_id)
            click_count_lst.append(click_count)
            obj_name_lst.append(obj_name)
            obj_scene_lst.append(obj_scene)


    df_obj_click_scene = pd.DataFrame({'物件ID': obj_id_lst,'物件名稱': obj_name_lst,'點擊量': click_count_lst,'物件場景': obj_scene_lst})
    df_obj_click_scene = df_obj_click_scene.dropna(subset=['物件名稱'])
    df_obj_click_scene = df_obj_click_scene[df_obj_click_scene['物件場景'].isin(scenes)]
    return df_obj_click_scene
