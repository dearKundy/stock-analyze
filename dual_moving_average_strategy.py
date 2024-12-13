import backtrader as bt
import pandas as pd
from sqlalchemy import create_engine
import json

# 创建数据库连接引擎
engine = create_engine("mysql+pymysql://root:root123456@localhost:3306/stock")

# 从MySQL中读取数据
def get_data_from_mysql(symbol):
    # 构建SQL查询语句
    query = f"SELECT date, open, high, low, close, volume FROM stock_k_his WHERE symbol = '{symbol}'"
    # 执行查询并将结果加载到DataFrame中
    df = pd.read_sql(query, engine, parse_dates=['date'])
    
    # 将volume字段的空值填充为0
    df['volume'] = df['volume'].fillna(0)
    
    # 确保volume为数值类型
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    # 将日期列设置为索引
    df.set_index('date', inplace=True)
    return df

def calculate_commission(price, shares, is_sell=False):
    """
    计算交易手续费
    :param price: 交易价格
    :param shares: 交易股数
    :param is_sell: 是否是卖出交易
    :return: 总手续费
    """
    # 佣金 = max(0.99, 0.0049 * shares)
    commission = max(0.99, 0.0049 * shares)
    
    # 平台���用费 = max(1.00, 0.005 * shares)
    platform_fee = max(1.00, 0.005 * shares)
    
    # 交收费 = 0.003 * shares
    settlement_fee = 0.003 * shares
    
    # 只有卖出时才收取的费用
    if is_sell:
        # 证监会规费 = max(0.01, 0.0000278 * price * shares)
        sec_fee = max(0.01, 0.0000278 * price * shares)
        
        # 交易活动费 = min(8.30, max(0.01, 0.000166 * shares))
        activity_fee = min(8.30, max(0.01, 0.000166 * shares))
    else:
        sec_fee = 0
        activity_fee = 0
    
    total_fee = commission + platform_fee + settlement_fee + sec_fee + activity_fee
    return total_fee

# 定义双均线策略类
class DualMovingAverageStrategy(bt.Strategy):
    params = (
        ('short_period', 10),
        ('long_period', 20),
        ('investment', 100),  # 每次买入金额
    )

    @staticmethod
    def get_strategy_name(short_period=10, long_period=20):
        return f"双均线交易策略 (MA{short_period} & MA{long_period})"

    def __init__(self):
        # 保持原有的均线初始化
        self.short_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_period)
        self.long_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_period)
        
        # 添加交易记录列表
        self.trades = []
        self.trade_data = {
            'dates': [],
            'prices': [],
            'short_ma': [],
            'long_ma': [],
            'signals': [],
            'volume': [],
            'commissions': []  # 添加手续费记录
        }

    def next(self):
        # 记录每日数据
        self.trade_data['dates'].append(self.data.datetime.date(0).isoformat())
        # 记录完整的OHLC数据
        self.trade_data['prices'].append({
            'open': float(self.data.open[0]),
            'high': float(self.data.high[0]),
            'low': float(self.data.low[0]),
            'close': float(self.data.close[0])
        })
        self.trade_data['short_ma'].append(float(self.short_ma[0]))
        self.trade_data['long_ma'].append(float(self.long_ma[0]))
        try:
            volume = float(self.data.volume[0])
        except (ValueError, TypeError):
            volume = 0
        self.trade_data['volume'].append(volume)
        
        # 交易逻辑
        if self.short_ma > self.long_ma and not self.position:
            price = float(self.data.close[0])
            # 计算可以买入的股数（假设每次投入100美元）
            investment = 100
            shares = investment / price
            self.buy(size=shares)
            self.trade_data['signals'].append(1)
            # 计算买入手续费
            commission = calculate_commission(price, shares, is_sell=False)
            self.trade_data['commissions'].append(commission)
            
        elif self.short_ma < self.long_ma and self.position:
            price = float(self.data.close[0])
            shares = self.position.size  # 获取当前持仓数量
            self.sell(size=shares)
            self.trade_data['signals'].append(-1)
            # 计算卖出手续费
            commission = calculate_commission(price, shares, is_sell=True)
            self.trade_data['commissions'].append(commission)
            
        else:
            self.trade_data['signals'].append(0)
            self.trade_data['commissions'].append(0)

# 添加新的均线突破策略类
class MABreakoutStrategy(bt.Strategy):
    params = (
        ('ma_period', 5),  # 默认使用5日均线
        ('investment', 100),  # 每次买入金额
    )

    def __init__(self):
        # 计算移动平均线
        self.ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.ma_period)
        
        # 记录前一天的收盘价是在均线上方还是下方
        self.above_ma = False
        
        # 添加交易记录
        self.trades = []
        self.trade_data = {
            'dates': [],
            'prices': [],
            'ma': [],  # 记录均线值
            'signals': [],
            'volume': [],
            'commissions': []
        }

    def next(self):
        # 记录每日数据
        self.trade_data['dates'].append(self.data.datetime.date(0).isoformat())
        self.trade_data['prices'].append({
            'open': float(self.data.open[0]),
            'high': float(self.data.high[0]),
            'low': float(self.data.low[0]),
            'close': float(self.data.close[0])
        })
        self.trade_data['ma'].append(float(self.ma[0]))
        try:
            volume = float(self.data.volume[0])
        except (ValueError, TypeError):
            volume = 0
        self.trade_data['volume'].append(volume)

        # 交易逻辑
        close = self.data.close[0]
        
        # 判断是否突破均线
        if close > self.ma[0] and not self.above_ma and not self.position:
            # 收盘价突破均线，买入
            shares = self.params.investment / close
            self.buy(size=shares)
            self.trade_data['signals'].append(1)
            # 计算买入手续费
            commission = calculate_commission(close, shares, is_sell=False)
            self.trade_data['commissions'].append(commission)
            self.above_ma = True
            
        elif close < self.ma[0] and self.above_ma and self.position:
            # 收盘价跌破均线，卖出
            shares = self.position.size
            self.sell(size=shares)
            self.trade_data['signals'].append(-1)
            # 计算卖出手续费
            commission = calculate_commission(close, shares, is_sell=True)
            self.trade_data['commissions'].append(commission)
            self.above_ma = False
            
        else:
            self.trade_data['signals'].append(0)
            self.trade_data['commissions'].append(0)

# 修改回测函数支持多策略
def run_backtest(symbol, strategy='dual_ma', short_period=10, long_period=20, ma_period=5, investment=100):
    cerebro = bt.Cerebro()
    
    # 根据策略名称选择不同的策略
    if strategy == 'dual_ma':
        cerebro.addstrategy(DualMovingAverageStrategy, 
                           short_period=short_period,
                           long_period=long_period,
                           investment=investment)
    elif strategy == 'ma_breakout':
        cerebro.addstrategy(MABreakoutStrategy,
                           ma_period=ma_period,
                           investment=investment)
    
    data = get_data_from_mysql(symbol)
    data_feed = bt.feeds.PandasData(dataname=data)
    
    cerebro.adddata(data_feed)
    cerebro.broker.setcash(10000.0)
    
    # 运行回测
    results = cerebro.run()
    strategy = results[0]
    
    # 导出数据
    output_data = {
        'symbol': symbol,
        'dates': strategy.trade_data['dates'],
        'prices': strategy.trade_data['prices'],
        'signals': strategy.trade_data['signals'],
        'commissions': strategy.trade_data['commissions'],
        'total_commission': sum(strategy.trade_data['commissions']),
        'volume': strategy.trade_data['volume']
    }
    
    # 根据策略类型添加不同的均线数据
    if isinstance(strategy, DualMovingAverageStrategy):
        output_data.update({
            'short_ma': strategy.trade_data['short_ma'],
            'long_ma': strategy.trade_data['long_ma']
        })
    elif isinstance(strategy, MABreakoutStrategy):
        output_data.update({
            'ma': strategy.trade_data['ma']
        })
    
    return output_data

# 运行回测，使用AAPL股票数据
run_backtest('AAPL') 
    