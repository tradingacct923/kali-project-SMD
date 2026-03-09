import os, sys, requests, time
sys.path.insert(0, r'C:\Users\aruna\OneDrive\war mechine\GreekSite')
from dotenv import load_dotenv
load_dotenv(r'C:\Users\aruna\OneDrive\war mechine\GreekSite\.env')

UN  = os.getenv('TOPSTEPX_USERNAME')
AK  = os.getenv('TOPSTEPX_API_KEY')
BASE = 'https://api.topstepx.com'
HUB  = 'https://rtc.topstepx.com/hubs/market'

r = requests.post(f'{BASE}/api/Auth/loginKey', json={'userName': UN, 'apiKey': AK}, timeout=10)
tok = r.json()['token']
CONTRACT_ID = 'CON.F.US.ENQ.H26'

received = []

from signalrcore.hub_connection_builder import HubConnectionBuilder

hub_url = f'{HUB}?access_token={tok}'
conn = (HubConnectionBuilder()
        .with_url(hub_url, options={'skip_negotiation': True, 'transport': 'websockets'})
        .build())

def on_depth(args):
    received.append(('depth', args))
    print('DEPTH:', str(args)[:120])

def on_trade(args):
    received.append(('trade', args))
    print('TRADE:', str(args)[:120])

def on_quote(args):
    received.append(('quote', args))
    print('QUOTE:', str(args)[:120])

def on_open():
    print('Connected! Subscribing...')
    conn.send('SubscribeContractQuotes',      [CONTRACT_ID])
    conn.send('SubscribeContractTrades',     [CONTRACT_ID])
    conn.send('SubscribeContractMarketDepth',[CONTRACT_ID])
    print('Subscribed to', CONTRACT_ID)

conn.on('GatewayDepth', on_depth)
conn.on('GatewayTrade', on_trade)
conn.on('GatewayQuote', on_quote)
conn.on_open(on_open)
conn.on_error(lambda e: print('ERR:', e))
conn.start()
time.sleep(15)

depth_count = sum(1 for x in received if x[0] == 'depth')
trade_count = sum(1 for x in received if x[0] == 'trade')
quote_count = sum(1 for x in received if x[0] == 'quote')
print(f'Done. Total={len(received)} depth={depth_count} trade={trade_count} quote={quote_count}')
conn.stop()
