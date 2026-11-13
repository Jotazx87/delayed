const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const NodeMediaServer = require('node-media-server');
const net = require('net');
const { spawn } = require('child_process');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

let previewClients = [];

app.get('/preview.flv', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'video/x-flv',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache'
  });
  previewClients.push(res);
  req.on('close', () => {
    previewClients = previewClients.filter((client) => client !== res);
  });
});

let config = {
  delaySeconds: 0,
  twitchKey: '',
  twitchServer: 'rtmp://gru01.contribute.live-video.net/app',
  kickUrl: '',
  kickKey: '',
  isLiveOut: false,
  obsConnected: false,
  bufferBytes: 0
};

let ffmpegProcess = null;
let bridgeFfmpeg = null;
let packetQueue = [];

const delayProxy = net.createServer((clientSocket) => {
  console.log('[Proxy] Fluxo FLV entrou no motor de delay.');
  clientSocket.on('data', (chunk) => {
    packetQueue.push({ data: chunk, timestamp: Date.now() });
    config.bufferBytes += chunk.length;
  });
  clientSocket.on('close', () => {
    console.log('[Proxy] Fluxo FLV encerrado.');
    packetQueue = [];
    config.bufferBytes = 0;
  });
  clientSocket.on('error', (err) => console.error('[Proxy Erro]:', err.message));
});

delayProxy.listen(1936, '127.0.0.1');

setInterval(() => {
  const now = Date.now();
  const delayMs = config.delaySeconds * 1000;
  while (packetQueue.length > 0 && (now - packetQueue[0].timestamp) >= delayMs) {
    const packet = packetQueue.shift();
    config.bufferBytes -= packet.data.length;
    if (ffmpegProcess && ffmpegProcess.stdin.writable) {
      ffmpegProcess.stdin.write(packet.data);
    }
    previewClients.forEach((client) => {
      if (!client.writableEnded) client.write(packet.data);
    });
  }
}, 20);

const nms = new NodeMediaServer({
  rtmp: { port: 1935, chunk_size: 60000, gop_cache: false, ping: 30, ping_timeout: 60 },
  logType: 1
});

nms.on('prePublish', (id, StreamPath) => {
  console.log('[OBS] Sinal conectado:', StreamPath);
  config.obsConnected = true;
  io.emit('status', config);
});

nms.on('donePublish', (id, StreamPath) => {
  console.log('[OBS] Sinal desconectado:', StreamPath);
  config.obsConnected = false;
  io.emit('status', config);
});

nms.run();

function startOutputStream() {
  if (ffmpegProcess) return;
  const targets = [];
  if (config.twitchKey) targets.push(`[f=flv]${config.twitchServer}/${config.twitchKey}`);
  if (config.kickUrl && config.kickKey) {
    const cleanKickUrl = config.kickUrl.replace(/\/$/, '');
    targets.push(`[f=flv]${cleanKickUrl}/${config.kickKey}`);
  }
  bridgeFfmpeg = spawn('ffmpeg', ['-i', 'rtmp://127.0.0.1:1935/live/stream', '-c', 'copy', '-f', 'flv', 'tcp://127.0.0.1:1936']);
  bridgeFfmpeg.on('error', (err) => console.error('[Bridge Erro]:', err.message));
  const outputArgs = targets.length > 0
    ? ['-c', 'copy', '-f', 'tee', '-map', '0:v', '-map', '0:a', targets.join('|')]
    : ['-c', 'copy', '-f', 'null', '-'];
  ffmpegProcess = spawn('ffmpeg', ['-re', '-f', 'flv', '-i', 'pipe:0', ...outputArgs]);
  config.isLiveOut = true;
  io.emit('status', config);
  ffmpegProcess.on('close', (code) => {
    console.log(`[FFmpeg] Encerrado com código ${code}`);
    if (bridgeFfmpeg) { bridgeFfmpeg.kill(); bridgeFfmpeg = null; }
    ffmpegProcess = null;
    config.isLiveOut = false;
    io.emit('status', config);
  });
}

function stopOutputStream() {
  if (ffmpegProcess) { ffmpegProcess.kill('SIGINT'); ffmpegProcess = null; }
  if (bridgeFfmpeg) { bridgeFfmpeg.kill('SIGINT'); bridgeFfmpeg = null; }
  packetQueue = [];
  config.bufferBytes = 0;
  config.isLiveOut = false;
  io.emit('status', config);
}

io.on('connection', (socket) => {
  socket.emit('status', config);
  socket.on('updateDelay', (newDelay) => {
    config.delaySeconds = Math.max(0, Math.min(300, Number(newDelay)));
    console.log(`[Delay] Alterado ao vivo para: ${config.delaySeconds}s`);
    io.emit('status', config);
  });
  socket.on('saveKeys', (data) => {
    config.twitchKey = data.twitchKey.trim();
    config.kickUrl = data.kickUrl.trim();
    config.kickKey = data.kickKey.trim();
    io.emit('status', config);
  });
  socket.on('toggleStream', (action) => {
    if (action === 'start') startOutputStream();
    if (action === 'stop') stopOutputStream();
  });
});

setInterval(() => {
  io.emit('telemetry', {
    bufferMB: (config.bufferBytes / (1024 * 1024)).toFixed(2),
    queuePackets: packetQueue.length,
    delaySeconds: config.delaySeconds
  });
}, 1000);

server.listen(3000, () => {
  console.log('🚀 Painel de Controle rodando em http://localhost:3000');
});
