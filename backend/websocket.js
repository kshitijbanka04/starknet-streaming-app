import { WebSocketServer } from "ws";
import mongoose from "mongoose";
import fetch from "node-fetch";
import dotenv from "dotenv";
dotenv.config();

// MongoDB Connection Setup
const DB_URI = process.env.DB_URI;

async function connectDB() {
    try {
        await mongoose.connect(DB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            serverSelectionTimeoutMS: 5000,
        });
        mongoose.set("debug", false); // Set to false in production to improve performance
        console.log("✅ MongoDB Connected");
    } catch (err) {
        console.error("❌ MongoDB Connection Error:", err);
        process.exit(1);
    }
}

await connectDB();

// Schemas & Models
const Bot = mongoose.model("Bot", new mongoose.Schema({
    bot_address: String,
    player: String,
    status: String,
    score: Number,
    starting_tile: String,
}));

const Mine = mongoose.model("Mine", new mongoose.Schema({
    bot_address: String,
    location: String,
    mine_type: String, // Diamond, Bomb, or Empty
    timestamp: Date,
}));

const Transaction = mongoose.model("Transaction", new mongoose.Schema({
    block: Number,
    event_name: String,
    event_hash: String,
    data: [String],
    timestamp: Date,
}));

// WebSocket Servers
const TRANSACTION_PORT = process.env.TRANSACTION_PORT;
const STATS_PORT = process.env.STATS_PORT;
const TILES_PORT = process.env.TILES_PORT || 3003; // New port for tile data
const TRANSACTION_API_URL = process.env.TRANSACTION_API_URL;

const EVENT_MAP = {
    "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3": "SpawnedBot",
    "0xd5efc9cfb6a4f6bb9eae0ce39d32480473877bb3f7a4eaa3944c881a2c8d25": "TileMined",
    "0x111861367b42e77c11a98efb6d09a14c2dc470eee1a4d2c3c1e8c54015da2e5": "BombFound",
    "0x14528085c8fd64b9210572c5b6015468f8352c17c9c22f5b7aa62a55a56d8d7": "DiamondFound",
    "0x1b74d97806c93468070e49a1626aba00f8e89dfb07246492af4566f898de982": "TileAlreadyMined",
    "0x1dcca826eea45d96bfbf26e9aabf510e94c6de62d0ce5e5b6e60c51c7640af8": "SuspendBot",
    "0x1d6a6a42fd13b206a721dbca3ae720621707ef3016850e2c5536244e5a7858a": "ReviveBot"
};

// Memory-efficient TransactionManager with rate limiting
class TransactionManager {
    constructor() {
        this.transactionQueue = [];
        this.continuationToken = null;
        this.isFetching = false;
        this.connectedClients = new Map(); // Changed to Map to store client-specific info
        this.pollInterval = null;
        this.maxQueueSize = 5000; // Limit queue size to prevent memory issues
    }

    async fetchTransactions(limit = 200) {
        if (this.isFetching) return;
        if (this.transactionQueue.length >= this.maxQueueSize) {
            console.log(`Queue at capacity (${this.transactionQueue.length}). Skipping fetch.`);
            return;
        }

        this.isFetching = true;

        try {
            const payload = {
                id: 1,
                jsonrpc: "2.0",
                method: "starknet_getEvents",
                params: [{
                    from_block: "pending",
                    to_block: "pending",
                    chunk_size: limit
                }]
            };

            if (this.continuationToken) {
                payload.params[0].continuation_token = this.continuationToken;
            }

            const response = await fetch(TRANSACTION_API_URL, {
                method: "POST",
                headers: { 
                    "accept": "application/json", 
                    "content-type": "application/json" 
                },
                body: JSON.stringify(payload)
            });

            const result = await response.json();
            
            if (!result?.result?.events) {
                console.error("Invalid response format:", result);
                return;
            }

            const transactions = result.result.events;
            
            if (transactions.length > 0) {
                this.continuationToken = result.result.continuation_token;

                // Process only most recent transactions if queue getting too large
                const formattedTransactions = transactions
                    .slice(0, Math.min(transactions.length, this.maxQueueSize - this.transactionQueue.length))
                    .map(tx => ({
                        event_name: EVENT_MAP[tx.keys[0]] || "TransferEvent",
                        data: tx.data,
                        timestamp: Date.now()
                    }));

                this.transactionQueue.push(...formattedTransactions);
                console.log(`Fetched ${formattedTransactions.length} transactions. Queue size: ${this.transactionQueue.length}`);
            } else {
                console.log("No new transactions found");
            }

        } catch (error) {
            console.error("Error fetching transactions:", error);
        } finally {
            this.isFetching = false;
        }
    }

    startPolling() {
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
        }

        // Perform initial fetch
        this.fetchTransactions();
        
        // Poll at a reasonable rate
        this.pollInterval = setInterval(() => this.fetchTransactions(), 1000);
    }

    startStreaming(ws, clientId) {
        // Store client info with rate limiting parameters
        this.connectedClients.set(ws, {
            id: clientId,
            batchSize: 10,
            nextBatchTime: Date.now(),
            batchInterval: 100,
            queueBacklog: [] // Client-specific queue for catching up
        });
        
        if (this.connectedClients.size === 1) {
            this.startPolling();
        }

        this.sendBatchToClient(ws);
    }

    sendBatchToClient(ws) {
        if (!this.connectedClients.has(ws) || ws.readyState !== 1) {
            this.connectedClients.delete(ws);
            return;
        }

        const clientInfo = this.connectedClients.get(ws);
        const now = Date.now();
        
        // Check if it's time to send next batch
        if (now >= clientInfo.nextBatchTime) {
            // First check client's backlog
            if (clientInfo.queueBacklog.length > 0) {
                const batchSize = Math.min(clientInfo.queueBacklog.length, clientInfo.batchSize);
                const batch = clientInfo.queueBacklog.splice(0, batchSize);
                
                try {
                    ws.send(JSON.stringify({
                        type: "transactions",
                        data: batch
                    }));
                } catch (error) {
                    console.error("Error sending to WebSocket:", error);
                    this.connectedClients.delete(ws);
                    return;
                }
            }
            // Then check main queue
            else if (this.transactionQueue.length > 0) {
                const batchSize = Math.min(this.transactionQueue.length, clientInfo.batchSize);
                const batch = this.transactionQueue.splice(0, batchSize);
                
                try {
                    ws.send(JSON.stringify({
                        type: "transactions",
                        data: batch
                    }));
                    
                    // Update all other clients' backlogs
                    for (const [otherWs, otherClientInfo] of this.connectedClients.entries()) {
                        if (otherWs !== ws) {
                            otherClientInfo.queueBacklog.push(...batch);
                            // Cap backlog size to prevent memory issues
                            if (otherClientInfo.queueBacklog.length > 1000) {
                                otherClientInfo.queueBacklog = otherClientInfo.queueBacklog.slice(-1000);
                            }
                        }
                    }
                } catch (error) {
                    console.error("Error sending to WebSocket:", error);
                    this.connectedClients.delete(ws);
                    return;
                }
            }

            // Calculate next batch time based on queue length
            const queueFactor = this.transactionQueue.length > 1000 ? 0.5 : 
                               this.transactionQueue.length > 500 ? 0.7 : 1;
            
            clientInfo.nextBatchTime = now + (clientInfo.batchInterval * queueFactor);
        }

        // Schedule next batch
        setTimeout(() => this.sendBatchToClient(ws), 50);
    }

    removeClient(ws) {
        this.connectedClients.delete(ws);
        
        if (this.connectedClients.size === 0) {
            if (this.pollInterval) {
                clearInterval(this.pollInterval);
                this.pollInterval = null;
            }
            this.transactionQueue = [];
            this.continuationToken = null;
        }
    }
}

// TileManager for handling tile data efficiently
class TileManager {
    constructor() {
        this.activeViewers = new Map(); // Maps client to their current active tile range
        this.updateInterval = null;
    }

    // Get mine data for specific layer and tile range
    async getTileData(layer, tileRange) {
        try {
            // Parse the tileRange string to get start and end IDs
            const [rangeStart, rangeEnd] = tileRange.split('-').map(id => id.replace(/[()]/g, ''));
            
            console.log(`Range: ${rangeStart} to ${rangeEnd}`);
            
            // Get all the specific tile IDs in this range
            const tileIds = [];
            for (let i = parseInt(rangeStart); i <= parseInt(rangeEnd); i++) {
                // Convert each individual tile ID to hex if needed
                const hexTileId = "0x" + i.toString(16);
                tileIds.push(hexTileId);
            }
            
            console.log(`Looking for these specific tiles: ${tileIds}`);
            
            // Query the database for mines with these exact location values
            const mines = await Mine.find({
                location: { $in: tileIds }
            }).lean();
            
            console.log(`Found ${mines.length} mines in the range`);
            return mines;
        } catch (error) {
            console.error("Error fetching tile data:", error);
            return [];
        }
    }

    // Register a client for a specific layer and tile range
    registerClient(ws, layer, tileRange) {
        this.activeViewers.set(ws, { layer, tileRange, lastUpdate: Date.now() });
        
        // Start interval if this is the first client
        if (this.activeViewers.size === 1) {
            this.startUpdates();
        }
        
        // Send initial data
        this.sendInitialData(ws);
    }
    
    async sendInitialData(ws) {
        if (!this.activeViewers.has(ws)) return;
        
        const { layer, tileRange } = this.activeViewers.get(ws);
        const tileData = await this.getTileData(layer, tileRange);
        
        try {
            ws.send(JSON.stringify({
                type: "tileData",
                action: "initial",
                layer,
                tileRange,
                data: tileData
            }));
        } catch (error) {
            console.error("Error sending initial tile data:", error);
            this.activeViewers.delete(ws);
        }
    }
    
    // Update tile range for an existing client
    updateClientView(ws, layer, tileRange) {
        if (this.activeViewers.has(ws)) {
            this.activeViewers.set(ws, { layer, tileRange, lastUpdate: Date.now() });
            this.sendInitialData(ws);
        }
    }
    
    // Start sending updates to all clients
    startUpdates() {
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
        }
        
        this.updateInterval = setInterval(async () => {
            // Process each client
            for (const [ws, { layer, tileRange, lastUpdate }] of this.activeViewers.entries()) {
                if (ws.readyState !== 1) {
                    this.activeViewers.delete(ws);
                    continue;
                }
                
                try {
                    // Find updates since last check
                    const [rangeStart, rangeEnd] = tileRange.split('-').map(id => id.replace(/[()]/g, ''));
                    
                    const updates = await Mine.find({
                        location: { $gte: rangeStart, $lte: rangeEnd },
                        timestamp: { $gt: new Date(lastUpdate) }
                    }).lean();
                    
                    if (updates.length > 0) {
                        ws.send(JSON.stringify({
                            type: "tileData",
                            action: "update",
                            layer,
                            tileRange,
                            data: updates
                        }));
                        
                        // Update the last update time
                        const viewerInfo = this.activeViewers.get(ws);
                        viewerInfo.lastUpdate = Date.now();
                        this.activeViewers.set(ws, viewerInfo);
                    }
                } catch (error) {
                    console.error("Error sending tile updates:", error);
                }
            }
        }, 1000); // Check for updates every second
    }
    
    removeClient(ws) {
        this.activeViewers.delete(ws);
        
        if (this.activeViewers.size === 0 && this.updateInterval) {
            clearInterval(this.updateInterval);
            this.updateInterval = null;
        }
    }
}

// Initialize managers
const transactionManager = new TransactionManager();
const tileManager = new TileManager();

// Set up Transaction WebSocket server
const transactionWSS = new WebSocketServer({ port: TRANSACTION_PORT });
console.log(`🚀 Transaction WebSocket running on ws://localhost:${TRANSACTION_PORT}`);

let clientCounter = 0;
transactionWSS.on("connection", (ws) => {
    const clientId = ++clientCounter;
    console.log(`📡 New Transaction WebSocket Connection (Client #${clientId})`);

    // Start streaming for this client
    transactionManager.startStreaming(ws, clientId);

    ws.on("close", () => {
        console.log(`🔴 Transaction WebSocket Disconnected (Client #${clientId})`);
        transactionManager.removeClient(ws);
    });
});

// Set up Stats & Leaderboard WebSocket
const statsWSS = new WebSocketServer({ port: STATS_PORT });
console.log(`🚀 Stats & Leaderboard WebSocket running on ws://localhost:${STATS_PORT}`);

statsWSS.on("connection", (ws) => {
    console.log("📡 New Stats WebSocket Connection");

    ws.on("close", () => console.log("🔴 Stats WebSocket Disconnected"));

    async function sendStatsAndLeaderboard() {
        try {
            const totalPlayers = await Bot.distinct("player").then(players => players.length);
            const totalBots = await Bot.countDocuments({});
            const botsAlive = await Bot.countDocuments({ status: "alive" });
            const botsDead = totalBots - botsAlive;
            const diamondsMined = await Mine.countDocuments({ mine_type: "Diamond" });
            const totalTilesMined = await Mine.countDocuments({})

            const leaderboard = await Bot.aggregate([
                { $group: { _id: "$player", total_score: { $sum: "$score" } } },
                { $sort: { total_score: -1 } },
                { $limit: 10 }
            ]);

            ws.send(JSON.stringify({
                type: "stats",
                data: { totalPlayers, totalBots, botsAlive, botsDead, diamondsMined, totalTilesMined, leaderboard }
            }));
        } catch (error) {
            console.error("❌ Error fetching stats:", error);
        }
    }

    const interval = setInterval(sendStatsAndLeaderboard, 2000);
    ws.on("close", () => clearInterval(interval));
});

// Set up Tile Data WebSocket Server
const tilesWSS = new WebSocketServer({ port: TILES_PORT });
console.log(`🚀 Tile Data WebSocket running on ws://localhost:${TILES_PORT}`);

// In your WebSocket server file (websocket.js)
// Inside the tilesWSS connection handler, add more detailed logging:

tilesWSS.on("connection", (ws) => {
    console.log("📡 New Tile Data WebSocket Connection");
    
    // Handle messages from client to update view
    ws.on("message", (message) => {
        try {
            console.log("Received message from client:", message.toString());
            const parsedMessage = JSON.parse(message);
            console.log("Parsed message:", parsedMessage);
            
            const { action, layer, tileRange } = parsedMessage;
            
            if (action === "viewTiles" && layer && tileRange) {
                console.log(`Client viewing Layer ${layer}, Tile Range: ${tileRange}`);
                
                // Register or update the client's view
                if (tileManager.activeViewers.has(ws)) {
                    console.log("Updating existing client view");
                    tileManager.updateClientView(ws, layer, tileRange);
                } else {
                    console.log("Registering new client");
                    tileManager.registerClient(ws, layer, tileRange);
                }
            } else {
                console.log("Invalid or incomplete message:", parsedMessage);
            }
        } catch (error) {
            console.error("Error processing message:", error, message.toString());
        }
    });
    
    // More detailed handling for close events
    ws.on("close", (code, reason) => {
        console.log(`🔴 Tile Data WebSocket Disconnected. Code: ${code}, Reason: ${reason}`);
        tileManager.removeClient(ws);
    });
});