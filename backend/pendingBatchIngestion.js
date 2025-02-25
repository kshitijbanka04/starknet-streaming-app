import { RpcProvider } from "starknet";
import mongoose from "mongoose";
import dotenv from "dotenv"
dotenv.config();

const TRANSACTION_API_URL = process.env.TRANSACTION_API_URL;
const provider = new RpcProvider({ nodeUrl: TRANSACTION_API_URL });

// **MongoDB Connection**
const DB_URI = process.env.DB_URI;
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
    bot_address: { type: String, index: true },
    player: String,
    status: String,
    score: Number,
    starting_tile: String,
});
const Bot = mongoose.model("Bot", botSchema);

const mineSchema = new mongoose.Schema({
    bot_address: { type: String, index: true },
    location: String,
    mine_type: String, // Diamond, Bomb, or Empty
    timestamp: Date,
});
const Mine = mongoose.model("Mine", mineSchema);

const transactionSchema = new mongoose.Schema({
    block: { type: Number, index: true },
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

// **Batch Process Events**
async function processEventsBatch(events, blockNumber) {
    const bulkBots = [];
    const bulkMines = [];
    const bulkTransactions = [];

    for (const event of events) {
        const { key, data, timestamp } = event;
        const eventName = EVENT_MAP[key] || "TransferEvent";

        let bot_address = data[0];

        switch (key) {
            case "0xd5efc9cfb6a4f6bb9eae0ce39d32480473877bb3f7a4eaa3944c881a2c8d25": // TileMined
                bulkMines.push({
                    updateOne: {
                        filter: { bot_address, location: data[2] },
                        update: { 
                            $setOnInsert: {
                                bot_address,
                                location: data[2],
                                mine_type: "Empty",
                                timestamp
                            }
                        },
                        upsert: true
                    }
                });

                bulkBots.push({
                    updateOne: {
                        filter: { bot_address },
                        update: { $inc: { score: 10 } }
                    }
                });
                break;

            case "0x14528085c8fd64b9210572c5b6015468f8352c17c9c22f5b7aa62a55a56d8d7": // DiamondFound
                // For DiamondFound, we want to force update the mine type
                bulkMines.push({
                    updateOne: {
                        filter: { bot_address, location: data[2] },
                        update: { 
                            $set: {
                                bot_address,
                                location: data[2],
                                mine_type: "Diamond",
                                timestamp
                            }
                        },
                        upsert: true
                    }
                });

                bulkBots.push({
                    updateOne: {
                        filter: { bot_address },
                        update: { $inc: { score: 5000 } }
                    }
                });
                break;

            case "0x111861367b42e77c11a98efb6d09a14c2dc470eee1a4d2c3c1e8c54015da2e5": // BombFound
                bulkMines.push({
                    updateOne: {
                        filter: { bot_address, location: data[1] }, // BombFound has location at data[1]
                        update: { 
                            $set: {
                                bot_address,
                                location: data[1],
                                mine_type: "Bomb",
                                timestamp
                            }
                        },
                        upsert: true
                    }
                });

                bulkBots.push({
                    updateOne: {
                        filter: { bot_address },
                        update: { $set: { status: "dead" } }
                    }
                });
                break;

            case "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3": // SpawnedBot
                bulkBots.push({
                    updateOne: {
                        filter: { bot_address },
                        update: {
                            $set: {
                                bot_address,
                                player: data[1],
                                status: "alive",
                                score: 0,
                                starting_tile: data[2]
                            }
                        },
                        upsert: true
                    }
                });
                break;
        }

        bulkTransactions.push({
            insertOne: {
                document: { block: blockNumber, event_name: eventName, event_hash: key, data, timestamp }
            }
        });
    }

    // Execute batch updates with ordered: false for better performance
    if (bulkBots.length > 0) await Bot.bulkWrite(bulkBots, { ordered: false });
    if (bulkMines.length > 0) await Mine.bulkWrite(bulkMines, { ordered: false });
    if (bulkTransactions.length > 0) await Transaction.bulkWrite(bulkTransactions, { ordered: false });

    console.log(`✅ Processed ${events.length} events in Block ${blockNumber}`);
}

// **Fetch and Store Events in Batches**
async function fetchAndStoreEvents(blockNumber) {
    let continuationToken = null;
    let events = [];

    try {
        do {
            const response = await provider.getEvents({
                from_block: { block_number: blockNumber },
                to_block: { block_number: blockNumber },
                chunk_size: 1000,
                continuation_token: continuationToken,
            });

            events.push(...response.events.map(event => ({
                block: blockNumber,
                key: event.keys[0],
                data: event.data,
                timestamp: new Date().toISOString(),
            })));

            continuationToken = response.continuation_token || null;
        } while (continuationToken);

        if (events.length) await processEventsBatch(events, blockNumber);

        await updateCheckpoint(blockNumber);
    } catch (error) {
        console.error(`❌ Error fetching events for block ${blockNumber}:`, error);
    }
}

// **Fetch and Process Events Faster**
async function fetchLiveBlocks(startBlock) {
    while (true) {
        let latestBlock = await provider.getBlock("latest");
        let latestBlockNumber = latestBlock.block_number;

        for (let block = startBlock + 1; block <= latestBlockNumber; block++) {
            await fetchAndStoreEvents(block);
        }

        startBlock = latestBlockNumber;
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
}

// **Start Processing**
(async () => {
    const lastProcessedBlock = await getLastProcessedBlock();
    await fetchLiveBlocks(lastProcessedBlock);
})();
