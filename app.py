from flask import Flask, render_template, jsonify, request, send_from_directory
from dual_moving_average_strategy import run_backtest, DualMovingAverageStrategy
import backtrader as bt
import os
from sqlalchemy import create_engine, text

app = Flask(__name__, static_folder='static')

@app.route('/')
def index():
    return render_template('chart.html')

@app.route('/backtest/<symbol>')
def backtest(symbol):
    # 获取参数
    strategy = request.args.get('strategy', 'dual_ma')  # 默认使用双均线策略
    investment = float(request.args.get('investment', 100))
    
    if strategy == 'dual_ma':
        short_period = int(request.args.get('short_period', 10))
        long_period = int(request.args.get('long_period', 20))
        results = run_backtest(symbol, strategy='dual_ma', 
                             short_period=short_period, 
                             long_period=long_period,
                             investment=investment)
        strategy_name = f"双均线交易策略 (MA{short_period} & MA{long_period})"
    else:
        ma_period = int(request.args.get('ma_period', 5))
        results = run_backtest(symbol, strategy='ma_breakout',
                             ma_period=ma_period,
                             investment=investment)
        strategy_name = f"均线突破策略 (MA{ma_period})"
    
    results['strategy_name'] = strategy_name
    return jsonify(results)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                             'favicon.ico', mimetype='image/vnd.microsoft.icon')


if __name__ == '__main__':
    app.run(debug=True) 