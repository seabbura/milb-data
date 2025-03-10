import pandas as pd
import requests
import datetime
import io
import os


def milb_request(start_dt, end_dt):
    """Baseball Savantからデータを取得する関数"""
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=pitcher&game_date_gt={}&game_date_lt={}&type=details".format(start_dt, end_dt)
    s = requests.get(url, timeout=None).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))
    return data

def main():
    # データディレクトリがなければ作成
    os.makedirs("data", exist_ok=True)
    
    # 既存のデータを読み込む（ファイルが存在しない場合は空のDataFrameを作成）
    try:
        df_prev = pd.read_json("data/sample_name.json")
        print(f"Loaded {len(df_prev)} existing records")
    except (FileNotFoundError, ValueError):
        df_prev = pd.DataFrame(columns=["pitcher", "player_name", "p_throws"])
        print("No existing data found or file is empty. Starting fresh.")
    
    # 開始日と終了日をdatetime.date型で設定
    start_date = datetime.date(2025, 2, 20)
    end_date = datetime.date(2025, 3, 10)
    
    # 各日のデータを格納するためのリスト
    df_list = []
    current_date = start_date
    
    while current_date <= end_date:
        # 日付を文字列に変換（例："2025-02-22"）
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching data for {date_str}...")
        
        # １日のデータを取得
        df_day = milb_request(date_str, date_str)
        
        # データが取得できた場合のみリストに追加
        if not df_day.empty:
            df_list.append(df_day)
            print(f"Retrieved {len(df_day)} rows for {date_str}")
        else:
            print(f"No data available for {date_str}")
            
        # 次の日に進む
        current_date += datetime.timedelta(days=1)
    
    # 新しいデータがある場合のみ処理を続行
    if df_list:
        # 複数のDataFrameを連結
        df_total = pd.concat(df_list, ignore_index=True)
        
        # データの整形
        # ゲームタイプを"S"に限定
        df_total_chk = df_total[df_total["game_type"] == "S"]
        
        # トラッキングデータのないレコードの除外
        columns_to_check = ['release_speed', 'release_pos_x', 'release_pos_z', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z']
        df_total_chk = df_total_chk[~df_total_chk[columns_to_check].isna().any(axis=1)]
        
        # データの抽出
        df_total_chk = df_total_chk[["pitcher", "player_name", "p_throws"]]
        
        # 既存データと新しいデータを結合（正しいconcat構文を使用）
        df_combined = pd.concat([df_prev, df_total_chk], ignore_index=True)
        
        # 重複を削除（最新のデータを保持）
        df_combined = df_combined.drop_duplicates(subset=["pitcher"], keep="last")
        
        print(f"Added {len(df_combined) - len(df_prev)} new unique records")
        
        # エクスポート
        df_combined.to_json("data/sample_name.json", orient="records")
        print("Data exported successfully to data/sample_name.json")
    else:
        print("No new data to add. Keeping the existing data file.")

if __name__ == "__main__":
    main()