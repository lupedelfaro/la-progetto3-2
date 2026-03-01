import ccxt

# Lista asset principali (simboli umani)
ASSET_PRINCIPALI = ["BTC/USD", "ETH/USD", "ADA/USD", "SOL/USD", "XRP/USD"]

kraken = ccxt.kraken()
markets = kraken.load_markets()

print("Asset principali disponibili su Kraken (codici API):\n")
for asset in ASSET_PRINCIPALI:
    # markets[asset]["id"] contiene il codice "pair" API usato da Kraken
    market_info = markets.get(asset)
    if market_info:
        print(f"{asset}: {market_info['id']}")
    else:
        print(f"{asset}: NOT FOUND on Kraken")