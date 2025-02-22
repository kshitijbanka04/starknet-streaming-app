import WebSocket from "ws";

const transactionWS = new WebSocket("ws://localhost:8081");
const statsWS = new WebSocket("ws://localhost:8082");

// Listen for transaction updates
transactionWS.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log("📩 New Transaction Data:", data);
};

// Listen for stats & leaderboard updates
statsWS.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log("📊 Updated Stats & Leaderboard:", data);
};

// Handle connection events
transactionWS.onopen = () => console.log("✅ Connected to Transactions WebSocket");
statsWS.onopen = () => console.log("✅ Connected to Stats WebSocket");

// Handle errors
transactionWS.onerror = (err) => console.error("❌ Transaction WebSocket Error:", err);
statsWS.onerror = (err) => console.error("❌ Stats WebSocket Error:", err);

// Handle disconnects
transactionWS.onclose = () => console.log("🔴 Transaction WebSocket Disconnected");
statsWS.onclose = () => console.log("🔴 Stats WebSocket Disconnected");
