// Agentic OS — server demo in Node.js puro (nessuna dipendenza)
// Avvio: node server.js  →  http://localhost:3000

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

// ---------------------------------------------------------------------------
// "Kernel" agentico: agenti simulati, coda di task, bus di eventi (SSE)
// ---------------------------------------------------------------------------

const agents = [
  { id: 'atlas',  name: 'Atlas',  role: 'Planner',    status: 'idle', task: null, completed: 0 },
  { id: 'nova',   name: 'Nova',   role: 'Researcher', status: 'idle', task: null, completed: 0 },
  { id: 'forge',  name: 'Forge',  role: 'Coder',      status: 'idle', task: null, completed: 0 },
  { id: 'sentry', name: 'Sentry', role: 'Reviewer',   status: 'idle', task: null, completed: 0 },
];

const taskQueue = [];
let taskCounter = 0;
const sseClients = new Set();

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) res.write(payload);
}

function log(agentId, message) {
  broadcast('log', { time: new Date().toISOString(), agent: agentId, message });
}

// Pipeline di step simulati per ogni task, in base al ruolo dell'agente
const STEPS = {
  Planner:    ['Analizzo la richiesta', 'Scompongo in sotto-obiettivi', 'Definisco il piano', 'Delego gli step'],
  Researcher: ['Cerco fonti rilevanti', 'Leggo la documentazione', 'Estraggo i punti chiave', 'Sintetizzo i risultati'],
  Coder:      ['Leggo il codice esistente', 'Scrivo l\'implementazione', 'Eseguo i test', 'Rifinisco il codice'],
  Reviewer:   ['Controllo la correttezza', 'Verifico la sicurezza', 'Valuto le performance', 'Approvo il risultato'],
};

function assignTask(agent, task) {
  agent.status = 'working';
  agent.task = task;
  task.status = 'in_progress';
  task.agent = agent.id;
  broadcast('agents', agents);
  broadcast('tasks', taskQueue);
  log(agent.id, `Preso in carico il task #${task.id}: "${task.goal}"`);

  const steps = STEPS[agent.role];
  let i = 0;
  const interval = setInterval(() => {
    if (i < steps.length) {
      task.progress = Math.round(((i + 1) / steps.length) * 100);
      log(agent.id, `[${task.progress}%] ${steps[i]}…`);
      broadcast('tasks', taskQueue);
      i++;
    } else {
      clearInterval(interval);
      task.status = 'done';
      task.progress = 100;
      agent.status = 'idle';
      agent.task = null;
      agent.completed++;
      log(agent.id, `✔ Task #${task.id} completato`);
      broadcast('agents', agents);
      broadcast('tasks', taskQueue);
      scheduler();
    }
  }, 1200 + Math.random() * 800);
}

function scheduler() {
  const pending = taskQueue.filter(t => t.status === 'pending');
  const free = agents.filter(a => a.status === 'idle');
  while (pending.length && free.length) {
    assignTask(free.shift(), pending.shift());
  }
}

// ---------------------------------------------------------------------------
// HTTP server: statico + API
// ---------------------------------------------------------------------------

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    sseClients.add(res);
    res.write(`event: agents\ndata: ${JSON.stringify(agents)}\n\n`);
    res.write(`event: tasks\ndata: ${JSON.stringify(taskQueue)}\n\n`);
    req.on('close', () => sseClients.delete(res));
    return;
  }

  if (url.pathname === '/api/tasks' && req.method === 'POST') {
    let body = '';
    req.on('data', c => (body += c));
    req.on('end', () => {
      let goal;
      try {
        goal = JSON.parse(body).goal;
      } catch {
        res.writeHead(400).end(JSON.stringify({ error: 'JSON non valido' }));
        return;
      }
      if (!goal || !goal.trim()) {
        res.writeHead(400).end(JSON.stringify({ error: 'goal mancante' }));
        return;
      }
      const task = { id: ++taskCounter, goal: goal.trim(), status: 'pending', progress: 0, agent: null };
      taskQueue.push(task);
      broadcast('tasks', taskQueue);
      log('kernel', `Nuovo task #${task.id} in coda: "${task.goal}"`);
      scheduler();
      res.writeHead(201, { 'Content-Type': 'application/json' }).end(JSON.stringify(task));
    });
    return;
  }

  if (url.pathname === '/' || url.pathname === '/index.html') {
    const file = path.join(__dirname, 'public', 'index.html');
    fs.readFile(file, (err, data) => {
      if (err) return res.writeHead(500).end('Errore interno');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }).end(data);
    });
    return;
  }

  res.writeHead(404).end('Not found');
});

server.listen(PORT, () => {
  console.log(`🧠 Agentic OS in ascolto su http://localhost:${PORT}`);
});
