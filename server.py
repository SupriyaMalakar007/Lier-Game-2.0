"""Liar's Card - realtime multiplayer server (2-4 players, optional bots).

Rules are ported from the Telegram bot (bot.py). Run:  python server.py
"""
import asyncio
import json
import os
import random
import string
import time
import traceback
from pathlib import Path

from aiohttp import web, WSMsgType

rng = random.SystemRandom()  # OS entropy: unpredictable, no seed bias

RANKS = ["Ace", "King", "Queen"]
BOT_NAMES = ["Rook", "Vesper", "Mox", "Juno", "Sable", "Pike"]
TURN_SECONDS = 45
SPEED = float(os.getenv("SPEED", "1"))  # lower = faster pacing (used for tests)
HERE = Path(__file__).parent
ROOMS: dict = {}


async def nap(seconds):
    await asyncio.sleep(seconds * SPEED)


def new_deck():
    deck = ["King"] * 6 + ["Queen"] * 6 + ["Ace"] * 6 + ["Joker"] * 2 + ["DEVIL"]
    rng.shuffle(deck)
    return deck


def rid(n, alphabet=string.ascii_lowercase + string.digits):
    return "".join(rng.choices(alphabet, k=n))


class Player:
    def __init__(self, name, bot=False, ws=None):
        self.id = "p" + rid(5)
        self.token = rid(20, string.ascii_letters + string.digits)
        self.name = (name or "Player").strip()[:16] or "Player"
        self.bot, self.ws = bot, ws
        self.reset()

    def reset(self):
        self.hand, self.alive, self.shots, self.strikes = [], True, 0, 0
        self.bullet = rng.randint(1, 6)  # fixed chamber, never reset between rounds

    @property
    def connected(self):
        return self.bot or (self.ws is not None and not self.ws.closed)


async def send(ws, obj):
    try:
        await ws.send_str(json.dumps(obj))
    except Exception:
        pass


class Room:
    def __init__(self, code):
        self.code = code
        self.players: list[Player] = []
        self.host = None
        self.phase = "lobby"  # lobby | playing | over
        self.round, self.rank, self.cur = 0, None, None
        self.latest = None  # {"by", "cards", "claim"}
        self.deadline, self.winner, self.busy = None, None, False
        self.q: asyncio.Queue = asyncio.Queue()
        self.task = None

    # ---- helpers -------------------------------------------------------
    def get(self, pid):
        return next((p for p in self.players if p.id == pid), None)

    def alive(self):
        return [p for p in self.players if p.alive]

    def holders(self):
        return [p for p in self.alive() if p.hand]

    def nxt(self, pid, pool):
        """Next player after `pid` (seating order) that is inside `pool`."""
        ids = [p.id for p in self.players]
        i = ids.index(pid) if pid in ids else -1
        for s in range(1, len(ids) + 1):
            c = self.players[(i + s) % len(ids)]
            if c in pool:
                return c
        return None

    def snap(self, me):
        left = max(0, self.deadline - time.time()) if self.deadline else None
        L = self.latest
        return {
            "t": "state", "code": self.code, "phase": self.phase, "host": self.host,
            "you": me.id, "round": self.round, "rank": self.rank, "cur": self.cur,
            "left": left, "total": TURN_SECONDS, "busy": self.busy, "winner": self.winner,
            "latest": {"by": L["by"], "n": len(L["cards"]), "claim": L["claim"]} if L else None,
            "hand": me.hand,
            "players": [
                {"id": p.id, "name": p.name, "bot": p.bot, "alive": p.alive,
                 "n": len(p.hand), "shots": p.shots, "on": p.connected}
                for p in self.players
            ],
        }

    async def push(self):
        for p in self.players:
            if p.ws is not None and not p.ws.closed:
                await send(p.ws, self.snap(p))

    async def emit(self, **ev):
        for p in self.players:
            if p.ws is not None and not p.ws.closed:
                await send(p.ws, {"t": "ev", **ev})

    # ---- game flow -----------------------------------------------------
    def start(self):
        for p in self.players:
            p.reset()
        # each player gets a different random bullet slot (1-6), fresh every game
        for p, b in zip(self.players, rng.sample(range(1, 7), len(self.players))):
            p.bullet = b
        if self.task and not self.task.done():
            self.task.cancel()
        self.phase, self.round, self.latest, self.winner = "playing", 0, None, None
        self.cur, self.deadline, self.busy = None, None, False
        self.task = asyncio.create_task(self.run())

    async def run(self):
        try:
            await self.new_round(self.players[0])
            while self.phase == "playing":
                if len(self.alive()) <= 1:
                    await self.finish()
                    break
                p = self.cur_player()
                if p is None:
                    await self.new_round(self.alive()[0])
                    continue
                await self.turn(p)
        except asyncio.CancelledError:
            raise
        except Exception:
            traceback.print_exc()

    def cur_player(self):
        p = self.get(self.cur)
        if p is None or not p.alive or not p.hand:
            h = self.holders()
            p = self.nxt(self.cur, h) if h else None
            self.cur = p.id if p else None
        return p

    async def new_round(self, starter):
        alive = self.alive()
        if len(alive) <= 1:
            return
        deck = new_deck()
        for p in self.players:
            p.hand = []
        for p in alive:
            p.hand = [deck.pop() for _ in range(5)]
        self.round += 1
        self.rank = rng.choice(RANKS)
        self.latest = None
        if starter is None or not starter.alive:
            starter = self.nxt(starter.id if starter else None, alive) or alive[0]
        self.cur, self.busy = starter.id, True
        await self.emit(k="round", n=self.round, rank=self.rank, starter=starter.id)
        await self.push()
        await nap(2.8)
        self.busy = False

    async def turn(self, p):
        while not self.q.empty():
            self.q.get_nowait()
        self.busy = False
        if p.bot:
            self.deadline = None
            await self.push()
            await nap(rng.uniform(1.5, 2.6))
            if p.alive and self.phase == "playing":
                await self.bot_act(p)
            return
        self.deadline = time.time() + (TURN_SECONDS if p.connected else 12) * max(SPEED, 0.02)
        await self.push()
        while True:
            left = max(self.deadline - time.time(), 0.01)
            try:
                pid, act = await asyncio.wait_for(self.q.get(), left)
            except asyncio.TimeoutError:
                await self.timeout(p)
                return
            if not p.alive or self.phase != "playing":
                return
            if pid != p.id:
                continue
            kind = act.get("t")
            if kind == "play":
                if await self.play(p, act.get("idx", [])):
                    return
                if p.ws is not None:
                    await send(p.ws, {"t": "err", "msg": "Invalid play. Pick 1 to 3 cards."})
                await self.push()
            elif kind == "liar":
                if self.latest and self.latest["by"] != p.id:
                    p.strikes = 0
                    await self.resolve(p)
                    return
            elif kind == "wake":
                return

    async def timeout(self, p):
        p.strikes += 1
        if p.strikes >= 2:
            await self.emit(k="toast", msg=f"{p.name} missed two turns and is out")
            await self.kill(p)
        else:
            await self.emit(k="toast", msg=f"{p.name} ran out of time (warning 1 of 2)")
        self.latest = None  # the window to challenge the previous play has closed
        n = self.nxt(p.id, self.holders())
        self.cur = n.id if n else None
        await self.push()

    async def kill(self, p):
        p.alive, p.hand = False, []
        await self.emit(k="out", id=p.id)

    async def play(self, p, idx):
        try:
            idx = sorted({int(i) for i in idx})
        except (TypeError, ValueError):
            return False
        if not 1 <= len(idx) <= 3 or idx[0] < 0 or idx[-1] >= len(p.hand):
            return False
        cards = [p.hand[i] for i in idx]
        for i in reversed(idx):
            p.hand.pop(i)
        p.strikes = 0
        self.latest = {"by": p.id, "cards": cards, "claim": self.rank}
        self.deadline = None
        others = [q for q in self.alive() if q is not p and q.hand]
        n = self.nxt(p.id, self.holders())
        # lock the UI and move the turn marker BEFORE broadcasting, so the player
        # never sees a stale "your turn" state (that made extra clicks vanish)
        self.busy = True
        self.cur = n.id if n else None
        await self.emit(k="play", by=p.id, n=len(cards), claim=self.rank)
        await self.push()
        await nap(0.9)
        if not p.hand and len(others) == 1:
            await self.emit(k="toast", msg="Last card played: automatic LIAR!")
            await self.push()
            await nap(1.4)
            await self.resolve(others[0])
            return True
        if n is None:
            await self.new_round(self.nxt(p.id, self.alive()))
        return True

    async def resolve(self, challenger):
        self.busy = True
        L = self.latest
        by = self.get(L["by"])
        cards = L["cards"]
        devil = "DEVIL" in cards
        valid = all(c in (self.rank, "Joker") for c in cards if c != "DEVIL")
        if devil:
            targets = [q for q in self.alive() if q is not by]
            if challenger in targets:
                targets.remove(challenger)
                targets.insert(0, challenger)
        else:
            targets = [challenger if valid else by]
        await self.push()
        await self.emit(k="reveal", by=by.id, challenger=challenger.id, cards=cards,
                        claim=L["claim"], valid=valid, devil=devil, targets=[t.id for t in targets])
        await nap(5.2 if devil else 4.6)
        self.latest = None
        for t in targets:
            if not t.alive:
                continue
            t.shots += 1
            hit = t.shots == t.bullet
            await self.emit(k="shot", id=t.id, n=t.shots, hit=hit)
            await nap(5.0)
            if hit:
                await self.kill(t)
            await self.push()
        if len(self.alive()) <= 1:
            await self.finish()
            return
        survivor = next((t for t in targets if t.alive), None)
        await self.new_round(survivor or self.nxt(targets[0].id, self.alive()))

    async def finish(self):
        al = self.alive()
        self.winner = al[0].id if al else None
        self.phase, self.cur, self.deadline, self.busy = "over", None, None, False
        await self.emit(k="win", id=self.winner)
        await self.push()

    async def bot_act(self, p):
        L = self.latest
        if L and L["by"] != p.id:
            n = len(L["cards"])
            if rng.random() < 0.18 + 0.12 * n:
                await self.emit(k="toast", msg=f"{p.name} calls LIAR!")
                await self.resolve(p)
                return
        hand = p.hand
        good = [i for i, c in enumerate(hand) if c in (self.rank, "Joker")]
        count = rng.randint(1, min(3, len(hand)))
        if good and count > len(good) and rng.random() < 0.7:
            count = len(good)
        if good and rng.random() >= 0.2:  # usually honest, sometimes bluff
            pick = rng.sample(good, min(count, len(good)))
            rest = [i for i in range(len(hand)) if i not in pick]
            rng.shuffle(rest)
            while len(pick) < count and rest:
                pick.append(rest.pop())
        else:
            pick = rng.sample(range(len(hand)), count)
        await self.play(p, pick)

    # ---- membership ----------------------------------------------------
    async def leave(self, p):
        if self.phase == "playing":
            if p.alive:
                await self.kill(p)
            p.ws = None
            self.q.put_nowait((self.cur, {"t": "wake"}))
        else:
            self.players.remove(p)
            if self.host == p.id:
                humans = [x for x in self.players if not x.bot]
                self.host = humans[0].id if humans else None
        if not any(x.connected and not x.bot for x in self.players):
            self.close()
        else:
            await self.push()

    def close(self):
        if self.task:
            self.task.cancel()
        ROOMS.pop(self.code, None)


# ---- websocket ---------------------------------------------------------
async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    me, room = None, None

    async def err(msg, fatal=False):
        await send(ws, {"t": "err", "msg": msg, "fatal": fatal})

    try:
        async for m in ws:
            if m.type != WSMsgType.TEXT:
                continue
            try:
                d = json.loads(m.data)
            except ValueError:
                continue
            t = d.get("t")

            if t == "create" and not room:
                code = rid(4, "ABCDEFGHJKLMNPQRSTUVWXYZ")
                while code in ROOMS:
                    code = rid(4, "ABCDEFGHJKLMNPQRSTUVWXYZ")
                room = ROOMS[code] = Room(code)
                me = Player(d.get("name"), ws=ws)
                room.players.append(me)
                room.host = me.id
            elif t == "join" and not room:
                r = ROOMS.get(str(d.get("code", "")).strip().upper())
                if not r:
                    await err("No room with that code.")
                    continue
                if r.phase != "lobby":
                    await err("That game has already started.")
                    continue
                if len(r.players) >= 4:
                    await err("Room is full (4 players max).")
                    continue
                room, me = r, Player(d.get("name"), ws=ws)
                room.players.append(me)
            elif t == "rejoin" and not room:
                r = ROOMS.get(str(d.get("code", "")).upper())
                p = next((x for x in r.players if x.token == d.get("token")), None) if r else None
                if not p:
                    await err("Session expired.", fatal=True)
                    continue
                old, p.ws = p.ws, ws
                room, me = r, p
                if old is not None and old is not ws and not old.closed:
                    await old.close()
            elif room and me:
                is_host = room.host == me.id
                if t == "addbot" and is_host and room.phase == "lobby" and len(room.players) < 4:
                    used = {p.name for p in room.players}
                    name = next((n for n in BOT_NAMES if n not in used), "Bot" + rid(2))
                    room.players.append(Player(name, bot=True))
                elif t == "rmbot" and is_host and room.phase == "lobby":
                    b = room.get(d.get("id"))
                    if b and b.bot:
                        room.players.remove(b)
                elif t == "start" and is_host and room.phase == "lobby":
                    if len(room.players) < 2:
                        await err("You need at least 2 players. Add a bot or invite a friend.")
                        continue
                    room.start()
                elif t in ("play", "liar") and room.phase == "playing":
                    room.q.put_nowait((me.id, d))
                elif t == "again" and room.phase == "over":
                    room.players = [p for p in room.players if p.connected]
                    if room.host not in [p.id for p in room.players if not p.bot]:
                        room.host = me.id
                    for p in room.players:
                        p.reset()
                    room.phase, room.round, room.latest, room.winner = "lobby", 0, None, None
                    room.cur, room.deadline, room.busy = None, None, False
                elif t == "leave":
                    r, p = room, me
                    room, me = None, None
                    await send(ws, {"t": "left"})
                    await r.leave(p)
                    continue
                else:
                    continue
            else:
                continue

            if t in ("create", "join", "rejoin"):
                await send(ws, {"t": "hello", "id": me.id, "token": me.token, "code": room.code})
            await room.push()
    finally:
        if room and me and me.ws is ws:
            me.ws = None
            if room.phase == "lobby":
                await room.leave(me)
            elif not any(x.connected and not x.bot for x in room.players):
                room.close()
            else:
                await room.push()
    return ws


async def index(_):
    return web.FileResponse(HERE / "index.html")


def make_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/healthz", lambda _: web.Response(text="ok"))
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    print(f"Liar's Card is running: http://localhost:{port}")
    web.run_app(make_app(), host="0.0.0.0", port=port, print=None)