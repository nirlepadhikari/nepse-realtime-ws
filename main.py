from fastapi import FastAPI
from aiohttp import ClientSession
import pandas as pd
import pusher

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
    pusher_client = pusher.Pusher(app_id=u'6022140857', key=u'1bc14f68-fef6-4627-b25b-a4d8668fa701', secret=u'{fE,g?*o%I=AGMOT|rB}|{4@{Kt;i%hI``DrFJT:qyA=RxZ["3mTq[(1WtQ04w1', cluster=u'ap-south-1',
                              host="container-service-1.i38vjrojbltka.ap-south-1.cs.amazonlightsail.com"
                              )
    pusher_client.trigger(u'liveData', u'priceChange', {'data': data})
    return data


@app.get("/health")
def read_health():
    return {"status": "healthy"}