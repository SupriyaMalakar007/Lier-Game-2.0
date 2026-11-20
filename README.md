# Liar's Card (animated multiplayer)

Browser version of your Telegram bot. 2-4 real players, optional bots, same rules as `bot.py`.

## Run on Windows

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python server.py
```

Open http://localhost:8080, enter a name, press **Create a room**.

## Let friends join

- Same Wi-Fi: send them `http://YOUR-PC-IP:8080` (find the IP with `ipconfig`).
- Over the internet: run `ngrok http 8080` and send the https link, or deploy the folder to
  any Python host (Render, Railway, Fly). Start command: `python server.py` (reads `PORT`).
- Share the 4-letter room code, or press **Copy invite link**.

## Lobby

The host sees **Add bot** on every open seat (and ✕ to remove one). The game needs 2 to 4 players in total, humans and bots combined.

## Notes

- Each player only receives their own cards; hands are never sent to other browsers.
- Rooms live in memory. Restarting the server ends active games.
- A refresh during a game rejoins your seat. Leaving mid-game counts as elimination.
- Turn timer: 45 s, two missed turns in a row eliminates you (as in the bot).
