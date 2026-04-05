const express = require('express')
const http = require('http')
const path = require('path')
const { WebSocketServer, WebSocket } = require('ws')

const app = express()
const server = http.createServer(app)
const wss = new WebSocketServer({ server })

// In-memory state
const games = new Map() // gameNumber -> { guesses: [], won: false, winner: null }
const clients = new Map() // gameNumber -> Set of ws connections

function getOrCreateGame (gameNumber) {
  if (!games.has(gameNumber)) {
    games.set(gameNumber, { guesses: [], won: false, winner: null })
  }
  return games.get(gameNumber)
}

function calcPoints (distance) {
  if (distance === 0) return 100
  if (distance <= 10) return 50
  if (distance <= 100) return 20
  if (distance <= 300) return 5
  return 0
}

function calcScores (guesses) {
  const scores = {}
  for (const g of guesses) {
    if (!scores[g.player]) scores[g.player] = 0
    scores[g.player] += calcPoints(g.distance)
  }
  return scores
}

function broadcast (gameNumber, message) {
  const connections = clients.get(gameNumber)
  if (!connections) return
  const data = JSON.stringify(message)
  for (const ws of connections) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data)
    }
  }
}

function getPlayers (gameNumber) {
  const connections = clients.get(gameNumber)
  if (!connections) return []
  const players = new Set()
  for (const ws of connections) {
    if (ws.player) players.add(ws.player)
  }
  return Array.from(players)
}

function stateMessage (gameNumber) {
  const game = getOrCreateGame(gameNumber)
  return {
    type: 'state',
    gameNumber,
    guesses: game.guesses,
    won: game.won,
    winner: game.winner,
    players: getPlayers(gameNumber),
    scores: calcScores(game.guesses)
  }
}

// Routes
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'))
})

app.get('/api/today', (req, res) => {
  const gameNumber = Math.floor((Date.now() - new Date('2022-09-18').getTime()) / 86400000)
  res.json({ gameNumber })
})

// WebSocket
wss.on('connection', (ws) => {
  ws.on('message', async (raw) => {
    let msg
    try {
      msg = JSON.parse(raw)
    } catch {
      return
    }

    if (msg.type === 'join') {
      const { gameNumber, player } = msg

      // Remove from previous game's client set
      if (ws.gameNumber !== undefined && ws.gameNumber !== gameNumber) {
        const prev = clients.get(ws.gameNumber)
        if (prev) {
          prev.delete(ws)
          broadcast(ws.gameNumber, { type: 'state', ...stateMessage(ws.gameNumber) })
        }
      }

      ws.gameNumber = gameNumber
      ws.player = player

      if (!clients.has(gameNumber)) {
        clients.set(gameNumber, new Set())
      }
      clients.get(gameNumber).add(ws)

      // Send full state to this client
      ws.send(JSON.stringify(stateMessage(gameNumber)))

      // Broadcast updated player list to all clients in the game
      broadcast(gameNumber, stateMessage(gameNumber))
    }

    if (msg.type === 'guess') {
      const { gameNumber, player, word } = msg

      if (!word || typeof word !== 'string' || !word.trim()) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid word.' }))
        return
      }

      const game = getOrCreateGame(gameNumber)

      if (game.won) {
        ws.send(JSON.stringify({ type: 'error', message: 'This game is already won!' }))
        return
      }

      const normalized = word.trim().toLowerCase()
      const url = `https://api.contexto.me/machado/en/game/${gameNumber}/${encodeURIComponent(normalized)}`

      let data
      try {
        const response = await fetch(url)
        if (!response.ok) throw new Error('API error')
        data = await response.json()
      } catch {
        ws.send(JSON.stringify({ type: 'error', message: 'Word not recognized. Try another.' }))
        return
      }

      const { distance, lemma } = data

      // Check if lemma already guessed
      if (game.guesses.some(g => g.word === lemma)) {
        ws.send(JSON.stringify({ type: 'error', message: 'That word (or its root form) has already been guessed!' }))
        return
      }

      // Add guess and sort by distance ascending
      game.guesses.push({ word: lemma, distance, player, timestamp: Date.now() })
      game.guesses.sort((a, b) => a.distance - b.distance)

      // Send result feedback to the guesser
      ws.send(JSON.stringify({ type: 'guessResult', word: lemma, distance }))

      if (distance === 0) {
        game.won = true
        game.winner = player
        broadcast(gameNumber, {
          type: 'won',
          gameNumber,
          winner: player,
          word: lemma,
          guesses: game.guesses,
          scores: calcScores(game.guesses)
        })
      } else {
        broadcast(gameNumber, stateMessage(gameNumber))
      }
    }
  })

  ws.on('close', () => {
    if (ws.gameNumber !== undefined) {
      const connections = clients.get(ws.gameNumber)
      if (connections) {
        connections.delete(ws)
        broadcast(ws.gameNumber, stateMessage(ws.gameNumber))
      }
    }
  })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => {
  console.log(`Collaborative Contexto running on port ${PORT}`)
})
