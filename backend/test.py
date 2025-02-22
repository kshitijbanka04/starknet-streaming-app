import asyncio
import websockets

async def test_websocket():
    uri = "ws://localhost:7171"
    try:
        async with websockets.connect(uri) as ws:  # Remove subprotocols
            print("Connected to WebSocket!")
            await ws.send("ping")  # Try a basic WebSocket message
            response = await ws.recv()
            print(f"Received: {response}")
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(test_websocket())
