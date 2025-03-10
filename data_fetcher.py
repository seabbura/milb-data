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
    # "data/milb_2025" ディレクトリがなければ作成
    os.makedirs("data/milb_2025", exist_ok=True)
    
    # 保存先のパス設定
    name_file_path = "data/milb_2025/sample_name.json"
    ext_file_path = "data/milb_2025/sample_ext.json"
    
    # 既存のデータを読み込む（ファイルが存在しない場合は空のDataFrameを作成）
    try:
        df_name_prev = pd.read_json(name_file_path)
        print(f"Loaded {len(df_name_prev)} existing name records")
    except (FileNotFoundError, ValueError):
        df_name_prev = pd.DataFrame(columns=["pitcher", "player_name", "p_throws"])
        print("No existing name data found or file is empty. Starting fresh.")
        
    try:
        df_ext_prev = pd.read_json(ext_file_path)
        print(f"Loaded {len(df_ext_prev)} existing ext records")
    except (FileNotFoundError, ValueError):
        df_ext_prev = pd.DataFrame(columns=["pitcher", "pitch_type", "p_throws", "stand", "pfx_x", "pfx_z", 
                                           "release_speed", "release_spin_rate", "description", "spin_axis", 
                                           "plate_x", "plate_z", "balls", "strikes"])
        print("No existing ext data found or file is empty. Starting fresh.")
    
    # 日付範囲を「本日の2日前から本日まで」に設定
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=2)
    end_date = today
    
    print(f"Date range: {start_date} to {end_date}")
    
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
        
        ### name部分のデータ処理 ###
        # データの抽出 (name)
        df_name_chk = df_total_chk[["pitcher", "player_name", "p_throws"]]
        
        # 既存データと新しいデータを結合
        df_name_combined = pd.concat([df_name_prev, df_name_chk], ignore_index=True)
        
        # 重複を削除（最新のデータを保持）
        df_name_combined = df_name_combined.drop_duplicates(subset=["pitcher"], keep="last")
        print(f"Added {len(df_name_combined) - len(df_name_prev)} new unique name records")
        
        # エクスポート
        df_name_combined.to_json(name_file_path, orient="records")
        print(f"Data exported successfully to {name_file_path}")
        
        ### ext部分のデータ処理 ###
        # データの抽出 (ext)
        ext_columns = ["pitcher", "pitch_type", "p_throws", "stand", "pfx_x", "pfx_z", 
                      "release_speed", "release_spin_rate", "description", "spin_axis", 
                      "plate_x", "plate_z", "balls", "strikes"]
        
        # 必要なカラムが存在するか確認
        existing_columns = set(df_total_chk.columns)
        required_columns = set(ext_columns)
        missing_columns = required_columns - existing_columns
        
        if missing_columns:
            print(f"Warning: Missing columns in data: {missing_columns}")
            # 存在するカラムのみで処理
            valid_columns = [col for col in ext_columns if col in existing_columns]
            df_ext_chk = df_total_chk[valid_columns]
        else:
            df_ext_chk = df_total_chk[ext_columns]
        
        # 既存データと新しいデータを結合
        df_ext_combined = pd.concat([df_ext_prev, df_ext_chk], ignore_index=True)
        
        # エクスポート
        df_ext_combined.to_json(ext_file_path, orient="records")
        print(f"Added {len(df_ext_combined) - len(df_ext_prev)} new ext records")
        print(f"Data exported successfully to {ext_file_path}")
        
    else:
        print("No new data to add. Keeping the existing data files.")


if __name__ == "__main__":
    main()