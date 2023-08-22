import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import geopandas as gpd
from shapely.geometry import Point
import folium
from streamlit_folium import folium_static
from folium.plugins import HeatMap
import geopy
from functions import *

st.set_page_config(
    page_title='光服務數據中心',
    layout='wide',
    initial_sidebar_state='collapsed'
    )

def main():
    today,yesterday,this_week_start,this_week_end,last_week_start,last_week_end,this_month_start,this_month_end,last_month_start,last_month_end = get_date_data()
    df_file = pd.read_csv("data/df_file.csv",encoding="utf-8-sig")  # 檔案資訊
    df_light = upload(df_file,"light")  # id <==> coordinate
    df_coor = upload(df_file,"coor")  # coordinate <==> scenes
    df_arobjs = upload(df_file,"arobjs")
    df_scan_coor_scene_city,df_coor_city,df_coor,df_arobjs = arrange_scan_data(df_light,df_coor,df_arobjs)

    
    #%%#【主頁面】 ============================================================================= ## 
    st.write(f"今天日期：{today}")
    st.markdown("<h4 style='text-align: center; background-color: #e6f2ff; padding: 10px;'>大稻埕掃描熱力圖</h4>", unsafe_allow_html=True)
    
    
    #%% 熱力圖
    
    ## back
    coor_list = ['大稻埕_貨櫃市集-1貓','大稻埕_貨櫃市集-3貓','大稻埕_迪化街-1','大稻埕_迪化街-2','大稻埕_迪化街-3','華英街-中']
    filtered_raw_df = df_scan_coor_scene_city[df_scan_coor_scene_city['coor_name'].isin(coor_list)]
    # 創建地圖
    m = folium.Map(location=[25.056779461206286, 121.50759925694413], zoom_start=15)
    # 創建 GeoDataFrame
    geometry = [Point(xy) for xy in zip(filtered_raw_df['經度'], filtered_raw_df['緯度'])]
    geo_df = gpd.GeoDataFrame(filtered_raw_df, geometry=geometry)
   
    earliest_time = filtered_raw_df['scantime'].min()
    now = datetime.now()
    default_start_date = now - timedelta(days=7)
    def date_filter(df,start_date,end_date):
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)   
        con1 = df['scantime'] >= start_date
        con2 = df['scantime'] <= end_date
        filtered_df = df[con1 & con2]
        return filtered_df

    # 進行座標分組和聚合
    col1 , col2 = st.columns(2)
    start_date = col1.date_input("輸入查詢開始日期", value=default_start_date)
    end_date = col2.date_input("輸入查詢結束日期", value=now)  

    filtered_df = date_filter(filtered_raw_df,start_date,end_date)
    
    coor_count = filtered_df.groupby(['coor_name', '緯度', '經度']).size().reset_index(name='count')
    
    # 將scantime轉換為日期格式，以便進行日期分組
    filtered_df['scantime'] = pd.to_datetime(filtered_df['scantime']).dt.date
    
    # 創建一個日期範圍
    date_range = pd.date_range(start=start_date, end=end_date,freq='D')
    
    # 創建一個空的Series來存儲每天的掃描量
    daily_scan_counts = pd.DataFrame(index=date_range, columns=coor_list)
    # 使用groupby計算每天每個地點的掃描量並填充到DataFrame中
    grouped = filtered_df.groupby(['scantime', 'coor_name']).size().unstack()
    daily_scan_counts.update(grouped)
    
    # 將缺失的日期填充為0
    daily_scan_counts.fillna(0, inplace=True)
    # 將索引（日期）格式化為 "m/d" 格式
    daily_scan_counts.index = daily_scan_counts.index.strftime('%m/%d %a')


    # 創建熱力圖數據
    heat_data = [[row['緯度'], row['經度'], row['count']] for index, row in coor_count.iterrows()]
    # 添加熱力圖到地圖
    HeatMap(heat_data, min_opacity=0.2, max_val=15, radius=15, blur=15).add_to(m)
    
    # 在每個熱力點上顯示數據數字
    for index, row in coor_count.iterrows():
        folium.Marker(
            [row['緯度'], row['經度']], 
            icon=None, 
            #popup=str(row['count']),
            popup=f"coor_name: {row['coor_name']}<br>Count: {row['count']}"
            ).add_to(m)  
    ##front 
  
 

    st.title("GPS數據熱力圖")
    st.write("下面是基於數據的熱力圖和地圖。")    
    folium_static(m)
    st.dataframe(daily_scan_counts.transpose())
    
    st.markdown("<h4 style='text-align: center; background-color: #e6f2ff; padding: 10px;'>大稻埕點擊數據</h4>", unsafe_allow_html=True)

    #backed
    select_coors = st.multiselect(
        label="選擇點擊物件查詢場域",
        options=coor_list,
        )
    default_start_date = now -timedelta(days=7)
    start_date_click = st.date_input(label='選擇點擊事件查詢日期',value = default_start_date) 
    end_date_click = st.date_input(label='選擇點擊事件查詢日期',value = now) 
    scenes_list = get_scenes(df_coor,select_coors)
    st.write(df_arobjs)
    st.write(start_date_click,end_date_click)
    st.write(scenes_list)
    df_obj_click_scene = get_GA_data(df_arobjs,start_date_click,end_date_click,scenes_list)
    df_obj_click_scene = df_obj_click_scene.set_index('物件ID')
    csv_scan_coor_scene_city = csv_download(df_obj_click_scene)
    
    #fronted
    #col_click , col_raw = st.columns(2)
    #with col_click:
    #st.markdown("<h5 style='text-align: left; padding: 10px;'>物件點擊排行榜</h5>", unsafe_allow_html=True)
    st.download_button(
     label = "下載物件點擊排行榜csv檔",
     data = csv_scan_coor_scene_city,
     file_name='點擊排行榜.csv',
     mime='text/csv',
     )
    st.table(
        data = df_obj_click_scene,
        )


    st.markdown("<h4 style='text-align: center; background-color: #e6f2ff; padding: 10px;'>大稻埕每日數據</h4>", unsafe_allow_html=True)
    
    #%% 按小時統計
    ##back
    select_coors = st.multiselect(
        label="選擇查詢場域",
        options=coor_list,
        )
    select_coors_string = ', '.join(map(str, select_coors))    
    selected_date = st.date_input(label='選擇欲查詢的日期',value = yesterday) 
    df_24hours,df_rawfilter = H24hour_scans(df_scan_coor_scene_city,selected_date,select_coors)
    fig_24hour = go.Figure()
    for coor in select_coors:
        fig_24hour.add_trace(go.Bar(
            x=df_24hours.index,
            y=df_24hours[coor],
            text=df_24hours[coor],
            name= coor,
        ))
    fig_24hour.update_layout(xaxis={'type': 'category'})

    fig_24hour.update_layout(
        title={
        'text': f"「{select_coors_string}」{selected_date}當日掃描量",
        'x': 0.5,
        'xanchor': 'center'
    },
    xaxis_title="時間",
    yaxis_title="掃描量",
    width=1000,
    height=400
    )
    st.plotly_chart(fig_24hour)
    
    start_date = selected_date
    end_date = selected_date
    csv_24hour = csv_download(df_24hours)
    

    
#%% Web App 測試 (檢視成果)  ============================================================================= ##    
if __name__ == "__main__":
    main()
    