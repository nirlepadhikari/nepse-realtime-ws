from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiohttp import ClientSession
import pandas as pd
import asyncio

app = FastAPI()

clients = set()  # Keep track of connected clients

async def scrape_site():
    url = "https://merolagani.com/LatestMarket.aspx"
    async with ClientSession() as session:
        async with session.get(url) as response:
            r = await response.text()
    df = pd.read_html(r)
    data = df[0].iloc[:,:7]
    data.columns = ['Symbol', 'LTP', 'Change', 'Open', 'High', 'Low', 'Volume']
    data = data.to_json(orient='records')
    return data

async def broadcast():
    """Broadcast the scraped data to all connected clients."""
    while True:
        data = await scrape_site()
        broadcast_coros = (client.send_text(data) for client in list(clients))
        await asyncio.gather(*broadcast_coros, return_exceptions=True)
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast())  # Start the broadcast task when the app starts

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    while True:
        try:
            await websocket.receive_text()  # Keep the connection open by awaiting for new messages
        except WebSocketDisconnect:
            clients.remove(websocket)
            break
