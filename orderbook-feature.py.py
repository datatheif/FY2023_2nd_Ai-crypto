import pandas as pd
from datetime import date
import math

# 1. 파일 읽어오기
df_o = pd.read_csv('2023-11-15-upbit-BTC-book.csv').apply(pd.to_numeric,errors='ignore')
group_o = df_o.groupby('timestamp')

df_t = pd.read_csv('2023-11-15-upbit-BTC-trade.csv').apply(pd.to_numeric, errors = 'ignore') 
group_t = df_t.groupby('timestamp')

# 2. mid_price 함수 정의
def cal_mid_price(gr_bid_level, gr_ask_level): 
    level = 5 
    # gr_rB = gr_bid_level.head(level)
    # gr_rT = gr_ask_level.head(level)
    
    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0: 

        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity

        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        
        mid_price = (bid_top_price + ask_top_price) * 0.5 
        
        return (mid_price)

    else:
        print ('Error: serious cal_mid_price')
        return (-1)
    

# 3. book_imbalance 함수 정의
def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    
    mid_price = mid

    ratio = param[0] # 0.2
    level = param[1] # 5
    interval = param[2] # 1
    
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 
    
     
    _flag = var['_flag']
        
    if _flag: #skipping first line
        var['_flag'] = False
        return 0.0

    quant_v_bid = gr_bid_level.quantity**ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity**ratio
    price_v_ask = gr_ask_level.price * quant_v_ask
 
    #quant_v_bid = gr_r[(gr_r['type']==0)].quantity**ratio
    #price_v_bid = gr_r[(gr_r['type']==0)].price * quant_v_bid

    #quant_v_ask = gr_r[(gr_r['type']==1)].quantity**ratio
    #price_v_ask = gr_r[(gr_r['type']==1)].price * quant_v_ask
        
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    askQty = quant_v_ask.values.sum()
    
    bid_ask_spread = interval  
    
    print(bidPx,bidQty, askPx, askQty)
        
    book_price = 0 #because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)

        
    # indicator_value = (book_price - mid_price) / bid_ask_spread
    indicator_value = (book_price - mid_price)
    print("indicator_value:", indicator_value, )
    
    return indicator_value

# 4. book_delta 함수 정의
def live_cal_book_d_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):

    #print gr_bid_level
    #print gr_ask_level

    ratio = param[0]; level = param[1]; interval = param[2]
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 

    decay = math.exp(-1.0/interval)
    
    
    _flag = var['_flag']
    prevBidQty = var['prevBidQty']
    prevAskQty = var['prevAskQty']
    prevBidTop = var['prevBidTop']
    prevAskTop = var['prevAskTop']
    bidSideAdd = var['bidSideAdd']
    bidSideDelete = var['bidSideDelete']
    askSideAdd = var['askSideAdd']
    askSideDelete = var['askSideDelete']
    bidSideTrade = var['bidSideTrade']
    askSideTrade = var['askSideTrade']
    bidSideFlip = var['bidSideFlip']
    askSideFlip = var['askSideFlip']
    bidSideCount = var['bidSideCount']
    askSideCount = var['askSideCount']
  
    curBidQty = gr_bid_level['quantity'].sum()
    curAskQty = gr_ask_level['quantity'].sum()
    curBidTop = gr_bid_level.iloc[0].price #what is current bid top?
    curAskTop = gr_ask_level.iloc[0].price

    #curBidQty = gr_r[(gr_r['type']==0)].quantity.sum()
    #curAskQty = gr_r[(gr_r['type']==1)].quantity.sum()
    #curBidTop = gr_r.iloc[0].price #what is current bid top?
    #curAskTop = gr_r.iloc[level].price


    if _flag:
        var['prevBidQty'] = curBidQty
        var['prevAskQty'] = curAskQty
        var['prevBidTop'] = curBidTop
        var['prevAskTop'] = curAskTop
        var['_flag'] = False
        return 0.0
    
    
    if curBidQty > prevBidQty:
        bidSideAdd += 1
        bidSideCount += 1
    if curBidQty < prevBidQty:
        bidSideDelete += 1
        bidSideCount += 1
    if curAskQty > prevAskQty:
        askSideAdd += 1
        askSideCount += 1
    if curAskQty < prevAskQty:
        askSideDelete += 1
        askSideCount += 1
        
    if curBidTop < prevBidTop:
        bidSideFlip += 1
        bidSideCount += 1
    if curAskTop > prevAskTop:
        askSideFlip += 1
        askSideCount += 1
        
    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = diff

    #_count_1 = (diff[(diff['type']==1)])['count'].reset_index(drop=True).get(0,0)
    #_count_0 = (diff[(diff['type']==0)])['count'].reset_index(drop=True).get(0,0)
    
    
    bidSideTrade += _count_1
    bidSideCount += _count_1
    
    askSideTrade += _count_0
    askSideCount += _count_0
    

    if bidSideCount == 0:
        bidSideCount = 1
    if askSideCount == 0:
        askSideCount = 1

    bidBookV = (-bidSideDelete + bidSideAdd - bidSideFlip) / (bidSideCount**ratio)
    askBookV = (askSideDelete - askSideAdd + askSideFlip ) / (askSideCount**ratio)
    tradeV = (askSideTrade/askSideCount**ratio) - (bidSideTrade / bidSideCount**ratio)
    bookDIndicator = askBookV + bidBookV + tradeV
    
        
    
    var['bidSideCount'] = bidSideCount * decay #exponential decay
    var['askSideCount'] = askSideCount * decay
    var['bidSideAdd'] = bidSideAdd * decay
    var['bidSideDelete'] = bidSideDelete * decay
    var['askSideAdd'] = askSideAdd * decay
    var['askSideDelete'] = askSideDelete * decay
    var['bidSideTrade'] = bidSideTrade * decay
    var['askSideTrade'] = askSideTrade * decay
    var['bidSideFlip'] = bidSideFlip * decay
    var['askSideFlip'] = askSideFlip * decay

    var['prevBidQty'] = curBidQty
    var['prevAskQty'] = curAskQty
    var['prevBidTop'] = curBidTop
    var['prevAskTop'] = curAskTop
    # var['df1'] = df1
 
    return bookDIndicator 


# 5. get_diff_count_units 함수 정의
def get_diff_count_units (diff):
    
    _count_1 = _count_0 = _units_traded_1 = _units_traded_0 = 0
    _price_1 = _price_0 = 0

    diff_len = len (diff)
    if diff_len == 1:
        row = diff.iloc[0]
        if row['type'] == 1:
            _count_1 = row['count']
            _units_traded_1 = row['units_traded']
            _price_1 = row['price']
        else:
            _count_0 = row['count']
            _units_traded_0 = row['units_traded']
            _price_0 = row['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    elif diff_len == 2:
        row_1 = diff.iloc[1]
        row_0 = diff.iloc[0]
        _count_1 = row_1['count']
        _count_0 = row_0['count']

        _units_traded_1 = row_1['units_traded']
        _units_traded_0 = row_0['units_traded']
        
        _price_1 = row_1['price']
        _price_0 = row_0['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

# 6. 변수 선언
param = 0.2,5,1
var_imbalance = {'_flag': True}
var_delta = {'_flag': True, 'prevBidQty': 0, 'prevAskQty': 0, 'prevBidTop': 0, 'prevAskTop': 0,\
             'bidSideAdd': 0, 'bidSideDelete': 0, 'askSideAdd': 0, 'askSideDelete': 0,\
             'bidSideTrade': 0, 'askSideTrade': 0, 'bidSideFlip': 0, 'askSideFlip': 0,\
             'bidSideCount': 0, 'askSideCount': 0}

# 7. dataframe 선언
_timestamp = list(group_o.groups.keys())
_mid_price = []
_book_imbalance = []
_book_delta = []

_dict_indicators = pd.DataFrame()
today = date.today()

# 8. 실행
keys = group_o.groups.keys()

for i in keys: 
    gr_o = group_o.get_group(i) 
    gr_bid_level = gr_o[(gr_o.type == 0)] 
    gr_ask_level = gr_o[(gr_o.type == 1)]
    
    gr_t = group_t.get_group(i)
    diff = get_diff_count_units(gr_t)    
    
    mid_price = cal_mid_price(gr_bid_level, gr_ask_level)
    book_imbalance = live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, var_imbalance, mid_price)
    book_delta = live_cal_book_d_v1(param, gr_bid_level, gr_ask_level, diff, var_delta, mid_price)
    
    _mid_price.append(mid_price)
    _book_imbalance.append(book_imbalance)
    _book_delta.append(book_delta)    
    
_dict_indicators['book_delta-0.2-5-1'] = _book_delta
_dict_indicators['book_imbalance-0.2-5-1'] = _book_imbalance
_dict_indicators['mid_price'] = _mid_price
_dict_indicators['timestamp'] = _timestamp

_dict_indicators.to_csv('{0}-{1}-{2}-exchange-market-feature.csv'.format(today.year, today.month, today.day), index=False)