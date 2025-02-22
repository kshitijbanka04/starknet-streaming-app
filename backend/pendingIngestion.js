import { RpcProvider } from "starknet";
import mongoose from "mongoose";

const provider = new RpcProvider({ nodeUrl: "https://madara-apex-htps-demo.karnot.xyz" });

// **MongoDB Connection**
const DB_URI = "mongodb://localhost:27017/starknet_game";
mongoose.connect(DB_URI, { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => console.log("✅ MongoDB Connected"))
    .catch(err => console.error("❌ MongoDB Connection Error:", err));

// **Schemas**
const checkpointSchema = new mongoose.Schema({
    key: { type: String, unique: true },
    value: mongoose.Schema.Types.Mixed,
});
const Checkpoint = mongoose.model("Checkpoint", checkpointSchema);

const botSchema = new mongoose.Schema({
    bot_address: String,
    player: String,
    status: String,
    score: Number,
    starting_tile: String,
});
const Bot = mongoose.model("Bot", botSchema);

const mineSchema = new mongoose.Schema({
    bot_address: String,
    location: String,
    mine_type: String, // Diamond, Bomb, or Empty
    timestamp: Date,
});
const Mine = mongoose.model("Mine", mineSchema);

const transactionSchema = new mongoose.Schema({
    block: Number,
    event_name: String,
    event_hash: String,
    data: [String],
    timestamp: Date,
});
const Transaction = mongoose.model("Transaction", transactionSchema);

// **Event Hash to Name Mapping**
const EVENT_MAP = {
    "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3": "SpawnedBot",
    "0xd5efc9cfb6a4f6bb9eae0ce39d32480473877bb3f7a4eaa3944c881a2c8d25": "TileMined",
    "0x111861367b42e77c11a98efb6d09a14c2dc470eee1a4d2c3c1e8c54015da2e5": "BombFound",
    "0x14528085c8fd64b9210572c5b6015468f8352c17c9c22f5b7aa62a55a56d8d7": "DiamondFound",
    "0x1b74d97806c93468070e49a1626aba00f8e89dfb07246492af4566f898de982": "TileAlreadyMined",
    "0x1dcca826eea45d96bfbf26e9aabf510e94c6de62d0ce5e5b6e60c51c7640af8": "SuspendBot",
    "0x1d6a6a42fd13b206a721dbca3ae720621707ef3016850e2c5536244e5a7858a": "ReviveBot"
};

// **Helper Functions**
async function getLastProcessedBlock() {
    const checkpoint = await Checkpoint.findOne({ key: "lastProcessedBlock" });
    return checkpoint ? checkpoint.value : 430; // Default block number if none found
}

async function updateCheckpoint(blockNumber) {
    await Checkpoint.updateOne({ key: "lastProcessedBlock" }, { value: blockNumber }, { upsert: true });
}

// **Process and Store Events**
async function processEvent(event) {
    const { block, key, data, timestamp } = event;
    const eventName = EVENT_MAP[key] || "TransferEvent";

    if (!EVENT_MAP[key]) return;

    let bot_address = data[0];
    let location = data[2];

    switch (key) {
        case "0xd5efc9cfb6a4f6bb9eae0ce39d32480473877bb3f7a4eaa3944c881a2c8d25": // TileMined
            let points = 10;
            let existingMine = await Mine.findOne({ bot_address, location });

            if (!existingMine) {
                await Mine.create({ bot_address, location, mine_type: "Empty", timestamp });

                let bot = await Bot.findOne({ bot_address });
                if (bot) {
                    bot.score += points;
                    await bot.save();
                }
            }
            break;

        case "0x14528085c8fd64b9210572c5b6015468f8352c17c9c22f5b7aa62a55a56d8d7": // DiamondFound
            let diamondPoints = 5000;
            await Mine.create({ bot_address, location, mine_type: "Diamond", timestamp });

            let botDiamond = await Bot.findOne({ bot_address });
            if (botDiamond) {
                botDiamond.score += diamondPoints;
                await botDiamond.save();
            }
            break;

        case "0x111861367b42e77c11a98efb6d09a14c2dc470eee1a4d2c3c1e8c54015da2e5": // BombFound
            await Mine.create({ bot_address, location: data[1], mine_type: "Bomb", timestamp });
            await Bot.updateOne({ bot_address }, { status: "dead" });
            break;

        case "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3": // SpawnedBot
            await Bot.create({ bot_address, player: data[1], status: "alive", score: 0, starting_tile: data[2] });
            break;
    }

    await Transaction.create({ block, event_name: eventName, event_hash: key, data, timestamp });
    console.log(`✅ Processed Event: ${eventName} in Block ${block}`);
}

// **Fetch and Process Events for a Given Block**
async function fetchAndStoreEvents(blockNumber) {
    let continuationToken = null;

    try {
        do {
            const response = await provider.getEvents({
                from_block: { block_number: blockNumber },
                to_block: { block_number: blockNumber },
                chunk_size: 1000,
                continuation_token: continuationToken,
            });

            for (const event of response.events) {
                await processEvent({
                    block: blockNumber,
                    key: event.keys[0],
                    data: event.data,
                    timestamp: new Date().toISOString(),
                });
            }

            continuationToken = response.continuation_token || null;
        } while (continuationToken);

        await updateCheckpoint(blockNumber);
    } catch (error) {
        console.error(`❌ Error fetching events for block ${blockNumber}:`, error);
    }
}

// **Step 1: Ingest Historic Data**
async function ingestHistoricData() {
    const lastProcessedBlock = await getLastProcessedBlock();
    
    let latestBlock = await provider.getBlock("latest");
    let latestBlockNumber = latestBlock.block_number;

    console.log(`🔄 Ingesting historic data from Block ${lastProcessedBlock + 1} to ${latestBlockNumber}...`);

    for (let block = lastProcessedBlock + 1; block <= latestBlockNumber; block++) {
        await fetchAndStoreEvents(block);
    }

    console.log("✅ Historic Data Ingestion Complete!");

    lastProcessedBlock = await getLastProcessedBlock();
    return lastProcessedBlock;
}

// **Step 2: Listen for New Blocks Dynamically**
async function fetchLiveBlocks(startBlock) {
    let lastProcessed = startBlock;

    while (true) {
        try {
            const latestBlock = await provider.getBlock("latest");
            const latestBlockNumber = latestBlock.block_number;

            if (latestBlockNumber > lastProcessed) {
                console.log(`📡 New blocks detected. Processing from ${lastProcessed + 1} to ${latestBlockNumber}...`);
                for (let block = lastProcessed + 1; block <= latestBlockNumber+1; block++) {
                    await fetchAndStoreEvents(block);
                }
                lastProcessed = latestBlockNumber;
            }
        } catch (error) {
            console.error("❌ Error fetching new blocks:", error);
        }

        await new Promise(resolve => setTimeout(resolve, 2000));
    }
}

async function getLatestBlockNumber() {
    try {
        const latestBlock = await provider.getBlock("latest");
        return latestBlock.block_number;
    } catch (error) {
        console.error("❌ Error fetching latest block:", error);
        throw error;
    }
}


async function startProcessing() {
    while (true) {
        try {
            // Check if chain is accessible
            await getLatestBlockNumber();
            
            console.log("🎯 Chain is accessible, starting event processing...");
            
            // Process historic data first
            const lastProcessedBlock = await ingestHistoricData();
            
            // Then switch to processing pending events
            await fetchLiveBlocks(lastProcessedBlock);
            
        } catch (error) {
            console.error("❌ Chain not accessible or error occurred:", error);
            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }
}

// Start the processing
startProcessing();
console.log("🚀 Event processor started...");