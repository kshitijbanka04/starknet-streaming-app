import { WebSocketServer } from "ws";
import mongoose from "mongoose";
import fetch from "node-fetch";
import dotenv from "dotenv"
dotenv.config();

// ✅ MongoDB Connection Setup
const DB_URI = process.env.DB_URI;

async function connectDB() {
    try {
        await mongoose.connect(DB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            serverSelectionTimeoutMS: 5000,
        });
        mongoose.set("bufferCommands", false);
        mongoose.set("debug", true);
        console.log("✅ MongoDB Connected");
    } catch (err) {
        console.error("❌ MongoDB Connection Error:", err);
        process.exit(1);
    }
}

await connectDB();

// ✅ Schemas & Models
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

// ✅ WebSocket Servers
const TRANSACTION_PORT = process.env.TRANSACTION_PORT;
const STATS_PORT = process.env.STATS_PORT;

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

// Queue to store and send transactions in FIFO order
class TransactionManager {
    constructor() {
        this.transactionQueue = [];
        this.continuationToken = null;
        this.isFetching = false;
        this.connectedClients = new Set();
        this.pollInterval = null;
        this.batchSize = 15; // 15 transactions per batch
        this.batchInterval = 50; // Send batches every 25ms (40 batches per second)
                                // This gives us 15*40 = 600 transactions per second
    }

    async fetchTransactions() {
        if (this.isFetching) return;
        this.isFetching = true;

        try {
            const payload = {
                id: 1,
                jsonrpc: "2.0",
                method: "starknet_getEvents",
                params: [{
                    from_block: "pending",
                    to_block: "pending",
                    chunk_size: 1000
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

                const formattedTransactions = transactions.map(tx => ({
                    event_name: EVENT_MAP[tx.keys[0]] || "TransferEvent",
                    data: tx.data,
                    timestamp: Date.now()
                }));

                this.transactionQueue.push(...formattedTransactions);
                console.log(`Fetched ${transactions.length} transactions. Queue size: ${this.transactionQueue.length}`);
                console.log(`Continuation token: ${this.continuationToken}`);
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

        const poll = async () => {
            await this.fetchTransactions();
            
            // If we have a small queue, fetch again immediately
            if (this.transactionQueue.length < 100) {
                setTimeout(() => this.fetchTransactions(), 50);
            }
        };

        poll();
        this.pollInterval = setInterval(poll, 500);
    }

    startStreaming(ws) {
        this.connectedClients.add(ws);
        
        if (this.connectedClients.size === 1) {
            this.startPolling();
        }

        const streamBatch = () => {
            if (!this.connectedClients.has(ws)) return;

            if (this.transactionQueue.length > 0) {
                let currentBatchSize;
                
                if (this.transactionQueue.length > 10000) {
                    currentBatchSize = 100; // Much larger batches to catch up
                } else if (this.transactionQueue.length > 1000) {
                    currentBatchSize = 50; // Larger batches when behind
                } else if (this.transactionQueue.length > 500) {
                    currentBatchSize = 25; // Moderately larger batches
                } else {
                    currentBatchSize = this.batchSize;
                }
                
                const batch = this.transactionQueue.splice(0, currentBatchSize);
                
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

            // Dynamic interval based on queue size
            const interval = this.transactionQueue.length > 1000 ? 25 :
                             this.transactionQueue.length > 500 ? 40 : this.batchInterval;
                             
            setTimeout(streamBatch, interval);
        };

        streamBatch();
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

const transactionManager = new TransactionManager();

// Set up WebSocket server
const transactionWSS = new WebSocketServer({ port: TRANSACTION_PORT });
console.log(`🚀 Transaction WebSocket running on ws://localhost:${TRANSACTION_PORT}`);

transactionWSS.on("connection", (ws) => {
    console.log("📡 New Transaction WebSocket Connection");

    // Start streaming for this client
    transactionManager.startStreaming(ws);

    ws.on("close", () => {
        console.log("🔴 Transaction WebSocket Disconnected");
        transactionManager.removeClient(ws);
    });
});


// **Stats & Leaderboard WebSocket**
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
