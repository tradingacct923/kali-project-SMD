"""
Raw frame inspector — connect to TopStepX and log ALL messages for 30s.
Shows what actually arrives so we can fix the connector.
"""
import os, sys, requests, time, json
sys.path.insert(0, r'C:\Users\aruna\OneDrive\war mechine\GreekSite')
from dotenv import load_dotenv
load_dotenv(r'C:\Users\aruna\OneDrive\war mechine\GreekSite\.env')

import websocket

UN  = os.getenv('TOPSTEPX_USERNAME')
AK  = os.getenv('TOPSTEPX_API_KEY')
BASE = 'https://api.topstepx.com'
HUB  = 'https://rtc.topstepx.com/hubs/market'

r = requests.post(f'{BASE}/api/Auth/loginKey', json={'userName': UN, 'apiKey': AK}, timeout=10)
tok = r.json()['token']
CONTRACT_ID = 'CON.F.US.ENQ.H26'

HANDSHAKE = '{"protocol":"json","version":1}\x1e'
SEP = '\x1e'

counts = {'text': 0, 'binary': 0, 'depth': 0, 'trade': 0, 'quote': 0, 'other': 0}
samples = {}

def on_message(ws, raw):
    if isinstance(raw, bytes):
        counts['binary'] += 1
        try:
            raw = raw.decode('utf-8')
        except:
            print(f'BINARY (undecodable) len={len(raw)}')
            return
    else:
        counts['text'] += 1

    parts = raw.split(SEP)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        try:
            data = json.loads(part)
        except Exception as e:
            print(f'MALFORMED ({len(part)}b): {part[:80]}')
            continue
        
        target = data.get('target', '')
        args = data.get('arguments', [])
        msg_type = data.get('type', 0)
        
        if target == 'GatewayDepth':
            counts['depth'] += 1
            if 'depth' not in samples:
                samples['depth'] = {'args_len': len(args), 'args_types': [type(a).__name__ for a in args], 'sample': str(args)[:200]}
                print(f'DEPTH SAMPLE: args_len={len(args)}, types={[type(a).__name__ for a in args]}')
                print(f'  args={str(args)[:200]}')
        elif target == 'GatewayTrade':
            counts['trade'] += 1
            if 'trade' not in samples:
                samples['trade'] = str(args)[:200]
                print(f'TRADE SAMPLE: args_len={len(args)}, types={[type(a).__name__ for a in args]}')
                print(f'  args={str(args)[:200]}')
        elif target == 'GatewayQuote':
            counts['quote'] += 1
            if 'quote' not in samples:
                samples['quote'] = str(args)[:200]
                print(f'QUOTE SAMPLE: args_len={len(args)}, types={[type(a).__name__ for a in args]}')
                print(f'  args={str(args)[:200]}')
        elif msg_type == 1:
            counts['other'] += 1
            print(f'OTHER: target={target} args_len={len(args)}')

def on_open(ws):
    print('Connected! Sending handshake...')
    ws.send(HANDSHAKE)
    time.sleep(0.5)
    for method in ('SubscribeContractQuotes', 'SubscribeContractTrades', 'SubscribeContractMarketDepth'):
        msg = json.dumps({'type': 1, 'target': method, 'arguments': [CONTRACT_ID]}) + SEP
        ws.send(msg)
    print(f'Subscribed to {CONTRACT_ID}. Listening for 30s...')

def on_error(ws, err):
    print(f'ERROR: {err}')

def on_close(ws, c, m):
    print(f'Closed: {c}')

hub_url = HUB.replace('https://', 'wss://') + f'?access_token={tok}'
ws = websocket.WebSocketApp(hub_url,
    on_open=on_open, on_message=on_message,
    on_error=on_error, on_close=on_close)

import threading
t = threading.Thread(target=ws.run_forever, kwargs={'ping_interval': 20}, daemon=True)
t.start()
time.sleep(30)
ws.close()
print(f'\n=== SUMMARY ===')
print(f'Text frames: {counts["text"]}, Binary frames: {counts["binary"]}')
print(f'GatewayDepth: {counts["depth"]}, GatewayTrade: {counts["trade"]}, GatewayQuote: {counts["quote"]}')
print(f'Samples: {json.dumps(samples, indent=2, default=str)}')
