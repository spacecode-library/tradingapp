# import MetaTrader5 as mt
# from datetime import datetime
# import pandas as pd
# mt.initialize()

# login = 52390766
# password = 'K0srJSL0j$dTgr'
# server = 'ICMarketsSC-Demo'

# mt.login(login,password,server)

# symbol = 'EURUSD'
# timeframe = mt.TIMEFRAME_M1
# date_from = datetime(2025,6,17)
# date_to = datetime.now()

# prices = mt.copy_rates_range(
#    symbol,       
#    timeframe,    
#    date_from,    
#    date_to
#    )

# print(prices)
# mt.shutdown()


# import MetaTrader5 as mt5
# from time import sleep
# from datetime import datetime

# # -------- CONFIGURE YOUR DETAILS --------
# MT5_LOGIN = 52390766            # Replace with your login ID
# MT5_PASSWORD = "K0srJSL0j$dTgr"  # Replace with your password
# MT5_SERVER = "ICMarketsSC-Demo`"  # e.g., 'ICMarketsSC-Demo'
# SYMBOL = "EURUSD"

# # -------- INITIALIZE MT5 CONNECTION --------
# if not mt5.initialize():
#     print("Initialization failed:", mt5.last_error())
#     quit()

# # Login (required only for some brokers)
# authorized = mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
# if not authorized:
#     print("Login failed:", mt5.last_error())
#     mt5.shutdown()
#     quit()

# # -------- REAL-TIME PRICE FETCHING --------
# try:
#     print(f"Fetching real-time prices for {SYMBOL}...\n")
#     while True:
#         tick = mt5.symbol_info_tick(SYMBOL)
#         if tick:
#             print(f"{datetime.now().strftime('%H:%M:%S')} | Bid: {tick.bid:.5f} | Ask: {tick.ask:.5f}")
#         else:
#             print("Failed to get tick data.")
#         sleep(1)  # Refresh every second

# except KeyboardInterrupt:
#     print("\nStopped by user.")
# finally:
#     mt5.shutdown()

import MetaTrader5 as mt5
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from typing import List
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

# --------- MT5 LOGIN CONFIG -------------
MT5_LOGIN = 52390766  # Replace with your actual login
MT5_PASSWORD = "K0srJSL0j$dTgr"
MT5_SERVER = "ICMarketsSC-Demo"
SYMBOLS = ["EURUSD", "USDJPY", "XAUUSD"]

# --------- WebSocket Connection Manager -------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print("Client connected:", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        print("Client disconnected:", len(self.active_connections))

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                self.active_connections.remove(connection)

manager = ConnectionManager()

# --------- Lifespan handler ---------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- On Startup ----
    if not mt5.initialize():
        raise RuntimeError("MT5 initialize failed: " + str(mt5.last_error()))
    if not mt5.login(MT5_LOGIN, MT5_PASSWORD, MT5_SERVER):
        raise RuntimeError("MT5 login failed: " + str(mt5.last_error()))

    async def broadcast_prices():
        while True:
            data = {}
            for symbol in SYMBOLS:
                tick = mt5.symbol_info_tick(symbol)
                if tick:
                    data[symbol] = {
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "bid": tick.bid,
                        "ask": tick.ask
                    }
            await manager.broadcast(str(data))
            await asyncio.sleep(1)

    asyncio.create_task(broadcast_prices())

    yield  # ---- Let FastAPI run ----

    # ---- On Shutdown ----
    mt5.shutdown()

# --------- FastAPI App Init with Lifespan ---------
app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/prices")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
