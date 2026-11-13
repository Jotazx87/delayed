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

// ==========================================
// PREVIEW MJPEG NATIVO
// ==========================================
let mjpegClients = [];
let mjpegBuffer = Buffer.alloc(0);

app.get('/preview.mjpg', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache'
  });

  mjpegClients.push(res);

  req.on('close', () => {
    mjpegClients = mjpegClients.filter((client) => client !== res);
  });
});

function broadcastJpegChunk(chunk) {
  mjpegBuffer = Buffer.concat([mjpegBuffer, chunk]);

  let start = mjpegBuffer.indexOf(Buffer.from([0xff, 0xd8]));
  let end = mjpegBuffer.indexOf(Buffer.from([0xff, 0xd9]), start + 2);

  while (start !== -1 && end !== -1) {
    const frame = mjpegBuffer.slice(start, end + 2);
    mjpegBuffer = mjpegBuffer.slice(end + 2);

    const header = `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${frame.length}\r\n\r\n`;
    mjpegClients.forEach((client) => {
      if (!client.writableEnded) {
        client.write(header);
        client.write(frame);
        client.write('\r\n');
      }
    });

    start = mjpegBuffer.indexOf(Buffer.from([0xff, 0xd8]));
    end = mjpegBuffer.indexOf(Buffer.from([0xff, 0xd9]), start + 2);
  }
}

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
  // Inicia o motor automaticamente assim que o sinal chega para liberar o preview
  if (!ffmpegProcess) {
    setTimeout(() => startOutputStream(), 500);
  }
});

nms.on('donePublish', (id, StreamPath) => {
  console.log('[OBS] Sinal desconectado:', StreamPath);
  config.obsConnected = false;
  stopOutputStream();
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

  bridgeFfmpeg = spawn('ffmpeg', [
    '-i', 'rtmp://127.0.0.1:1935/live/stream',
    '-c', 'copy',
    '-f', 'flv',
    'tcp://127.0.0.1:1936'
  ]);

  bridgeFfmpeg.on('error', (err) => console.error('[Bridge Erro]:', err.message));

  const args = [
    '-f', 'flv',
    '-i', 'pipe:0',
    '-map', '0:v',
    '-vf', 'fps=15,scale=640:-1',
    '-q:v', '7',
    '-f', 'mjpeg',
    'pipe:1'
  ];

  if (targets.length > 0) {
    args.push('-c', 'copy', '-f', 'tee', '-map', '0:v', '-map', '0:a', targets.join('|'));
  }

  ffmpegProcess = spawn('ffmpeg', args);

  ffmpegProcess.stdout.on('data', (chunk) => {
    broadcastJpegChunk(chunk);
  });

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
