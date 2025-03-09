import pandas as pd
import requests
import datetime
import io
import os

### MiLBのSavantデータ取得 ###
# 関数の定義
def milb_request(start_dt, end_dt):
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=pitcher&game_date_gt={}&game_date_lt={}&type=details".format(start_dt, end_dt)
    s = requests.get(url, timeout=None).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))
    return data

def main():
    # 開始日と終了日をdatetime.date型で設定（適宜変更してください）
    start_date = datetime.date(2025, 2, 20)
    end_date = datetime.date(2025, 2, 21)
    
    # 各日のデータを格納するためのリスト
    df_list = []
    current_date = start_date
    
    while current_date <= end_date:
        # 日付を文字列に変換（例："2024-03-20"）
        date_str = current_date.strftime("%Y-%m-%d")
        # １日のデータを取得（start_dtとend_dtに同じ日付を指定）
        df_day = milb_request(date_str, date_str)
        # データが取得できた場合のみリストに追加
        if not df_day.empty:
            df_list.append(df_day)
        # 次の日に進む
        current_date += datetime.timedelta(days=1)
    
    # 複数のDataFrameを連結
    if df_list:
        df_total = pd.concat(df_list, ignore_index=True)
    else:
        df_total = pd.DataFrame()
    
    ### データの整形 ###
    # ゲームタイプを"S"に限定
    df_total_chk = df_total[df_total["game_type"] == "S"]
    # トラッキングデータのないレコードの除外
    columns_to_check = ['release_speed', 'release_pos_x', 'release_pos_z', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z']
    df_total_chk = df_total_chk[~df_total_chk[columns_to_check].isna().any(axis=1)]
    
    ### データの抽出 ### 
    df_total_chk = df_total_chk[["pitcher", "player_name", "p_throws"]]
    df_total_chk = df_total_chk.drop_duplicates(subset=["pitcher"])
    
    ### エクスポート ###
    # data ディレクトリがなければ作成
    os.makedirs("data", exist_ok=True)
    df_total_chk.to_json("data/sample_name.json", orient="records")
    print("Data exported successfully to data/sample_name.json")

if __name__ == "__main__":
    main()