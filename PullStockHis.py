import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime
from config.db_config import DBConfig

def calculate_ma(data):
    """计算移动平均线"""
    data['ma5'] = data['close'].rolling(window=5).mean().round(2)
    data['ma10'] = data['close'].rolling(window=10).mean().round(2)
    data['ma20'] = data['close'].rolling(window=20).mean().round(2)
    data['ma30'] = data['close'].rolling(window=30).mean().round(2)
    
    data['ma50'] = data['close'].rolling(window=50).mean().round(2)
    data['ma120'] = data['close'].rolling(window=120).mean().round(2)
    data['ma240'] = data['close'].rolling(window=240).mean().round(2)
    data['ma360'] = data['close'].rolling(window=360).mean().round(2)
    
    return data

def get_market_data(ticker, start_date, end_date):
    try:
        ticker_obj = yf.Ticker(ticker)
        data = ticker_obj.history(start=start_date, end=end_date)
        
        if data.empty:
            print(f"警告: 未能获取到 {ticker} 的数据")
            return None
            
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        data.columns = data.columns.str.lower()
        data['symbol'] = ticker
        
        # 计算移动平均线
        data = calculate_ma(data)
        return data
    except Exception as e:
        print(f"获取 {ticker} 数据时发生错误: {str(e)}")
        return None

# 定义要获取的股票及其IPO日期
stocks = {
    "PLTR": "2020-09-30",
    "JPM": "1969-01-01",
    "BRK-B": "1996-05-09",
    "MSFT": "1986-03-13",
    "META": "2012-05-18",
    "GOOG": "2004-08-19",
    "V": "2008-03-19",
    "GS": "1999-05-04",
    "NVDA": "1999-01-22",
    "AMD": "1972-09-27",
    "WMT": "1972-08-25",
    "TSLA": "2010-06-29",
    "AAPL": "1980-12-12",
    "AMZN": "1997-05-15"
}

# 创建数据库连接
engine = create_engine(DBConfig.get_connection_url())

# 遍历每个股票
for symbol, start_date in stocks.items():
    print(f"\n开始处理 {symbol} 的数据...")
    
    # 获取数据并计算均线
    data = get_market_data(symbol, start_date, "2024-12-06")
    if data is not None:
        print(f"成功获取到 {len(data)} 条数据记录")
        
        try:
            # 先在事务外执行删除操作
            with engine.connect() as connection:
                delete_query = text("DELETE FROM stock_k_his WHERE symbol = :symbol")
                connection.execute(delete_query, {"symbol": symbol})
                connection.commit()
                print(f"已删除 {symbol} 的历史数据")
            
            # 然后在事务内执行插入操作
            with engine.connect() as connection:
                with connection.begin():
                    # 分批处理数据
                    BATCH_SIZE = 1000
                    total_records = len(data)
                    
                    for i in range(0, total_records, BATCH_SIZE):
                        batch = data.iloc[i:i + BATCH_SIZE]
                        # 将 NaN 值转换为 None
                        batch = batch.where(pd.notnull(batch), None)
                        batch.to_sql('stock_k_his', engine, if_exists='append', index=True, index_label='date')
                        print(f"已插入 {i + len(batch)} / {total_records} 条记录")
                    
                    print(f"成功插入所有 {total_records} 条 {symbol} 数据到数据库")
                    
        except Exception as e:
            print(f"处理 {symbol} 时发生错误: {str(e)}")
            continue

print("\n所有股票数据处理完成！")