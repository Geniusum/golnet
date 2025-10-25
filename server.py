import asyncio
import websockets
import json
import time

class GameServer:
    def __init__(self):
        self.server = None
        self.port = None
        self.clients = {}
        self.game_state = {
            "grid": {},
            "players": {},
            "game_started": False,
            "width": 128,
            "height": 128,
            "tick_time": 500,
            "game_duration": 300,
            "start_time": None,
            "tick_count": 0
        }
        self.max_players = 0
        self.game_loop_task = None
        self.state_lock = asyncio.Lock()

        self.loop_running = False
        self.min_players = 1
        self.wait_time = 180  
        self.main_loop_task = None
        self.lobby_timer_task = None
        self.lobby_end_time = None

    async def handle_client(self, websocket):
        """Handles client connections and messages."""
        try:
            async for message in websocket:
                data = json.loads(message)
                action = data.get("action")

                if action == "join":
                    await self.handle_join(websocket, data)
                elif websocket in self.clients:
                    player_id = self.clients[websocket]["id"]
                    if action == "place_preview":
                        cells = data.get("cells", [])
                        if isinstance(cells, list):
                            self.game_state["players"][player_id]["previews"].extend(cells)
                    elif action == "chat":
                        await self.broadcast_chat(player_id, data.get("message"))
                elif action == "get_status":
                    await self.handle_status_request(websocket)

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            player_id_disconnected = None
            async with self.state_lock:
                if websocket in self.clients:
                    player_id_disconnected = self.clients[websocket]["id"]
                    if player_id_disconnected in self.game_state["players"]:
                        del self.game_state["players"][player_id_disconnected]
                    del self.clients[websocket]
            
            if player_id_disconnected:
                print(f"Player {player_id_disconnected} disconnected.")
                await self.broadcast_player_list()


    async def handle_join(self, websocket, data):
        """Handles a player's attempt to join."""
        username = str(data.get("username", "")).strip()
        color = data.get("color")

        if self.game_state["game_started"]:
            await self.send_error(websocket, "A game is already in progress.")
            await websocket.close()
            return

        if self.max_players > 0 and len(self.game_state["players"]) >= self.max_players:
            await self.send_error(websocket, "The lobby is full.")
            await websocket.close()
            return

        if not (3 <= len(username) <= 32):
            await self.send_error(websocket, "Username must be between 3 and 32 characters.")
            await websocket.close()
            return
            
        if any(p['username'].lower() == username.lower() for p in self.game_state['players'].values()):
            await self.send_error(websocket, "This username is already taken.")
            await websocket.close()
            return

        async with self.state_lock:
            player_id = username
            self.clients[websocket] = {"id": player_id, "ws": websocket}
            self.game_state["players"][player_id] = {
                "id": player_id,
                "username": username,
                "color": color,
                "previews": [],
                "cells": 0
            }
            print(f"Player {username} has joined the lobby. Total players: {len(self.game_state['players'])}")
        
        await self.send_to_client(websocket, {"action": "joined"})
        await self.broadcast_player_list()
    
    

    async def main_loop(self):
        """The main loop that controls the game flow from lobby to game and back."""
        print("Game loop started. Waiting for players...")
        while self.loop_running:
            try:
                while len(self.game_state["players"]) < self.min_players:
                    if not self.loop_running: return
                    await asyncio.sleep(1)
                
                print(f"Minimum players ({self.min_players}) reached. Starting lobby timer ({self.wait_time}s).")

                self.lobby_end_time = time.time() + self.wait_time
                self.lobby_timer_task = asyncio.create_task(self.lobby_countdown())
                await self.lobby_timer_task
                self.lobby_end_time = None
                
                if len(self.game_state["players"]) >= self.min_players:
                    await self.start_game_logic()
                    if self.game_loop_task:
                        await self.game_loop_task  
                else:
                    print("Not enough players to start the game after countdown. Resetting.")

                print("Cycle finished. Waiting for players for the next game...")
                await asyncio.sleep(2) 

            except asyncio.CancelledError:
                print("Main game loop has been cancelled.")
                break
            except Exception as e:
                print(f"An error occurred in the main loop: {e}")
                await asyncio.sleep(5)

    async def lobby_countdown(self):
        """Counts down from wait_time, checking player count continuously."""
        try:
            for i in range(self.wait_time, 0, -1):
                if len(self.game_state["players"]) < self.min_players:
                    print(f"Player count dropped below {self.min_players}. Lobby timer reset.")
                    return 
                
                if i % 15 == 0 or i <= 5:
                    print(f"Game starting in {i} seconds...")

                await asyncio.sleep(1)
            print("Lobby timer finished.")
        except asyncio.CancelledError:
            print("Lobby countdown was cancelled.")

    async def game_loop(self):
        """Main game loop that manages ticks."""
        self.game_state["start_time"] = time.time()
        while self.game_state["game_started"]:
            try:
                start_tick_time = time.time()
                async with self.state_lock:
                    for player_id, player in self.game_state["players"].items():
                        for cell in player.get("previews", []):
                            pos = f"{cell['x']},{cell['y']}"
                            if pos not in self.game_state["grid"]:
                                self.game_state["grid"][pos] = {"player": player_id, "color": player["color"]}
                        player["previews"] = []

                    new_grid = {}
                    all_cells_to_check = set(self.game_state["grid"].keys())
                    
                    for pos_str in list(all_cells_to_check):
                        x, y = map(int, pos_str.split(','))
                        for i in range(-1, 2):
                            for j in range(-1, 2):
                                all_cells_to_check.add(f"{x+i},{y+j}")

                    for pos_str in all_cells_to_check:
                        x, y = map(int, pos_str.split(','))
                        neighbors = []
                        neighbor_owners = {}

                        for i in range(-1, 2):
                            for j in range(-1, 2):
                                if i == 0 and j == 0:
                                    continue
                                neighbor_pos = f"{x+i},{y+j}"
                                if neighbor_pos in self.game_state["grid"]:
                                    owner = self.game_state["grid"][neighbor_pos]["player"]
                                    if owner in self.game_state["players"]:
                                        neighbors.append(owner)
                                        neighbor_owners[owner] = neighbor_owners.get(owner, 0) + 1
                        
                        num_neighbors = len(neighbors)
                        is_alive = pos_str in self.game_state["grid"]

                        if is_alive and (num_neighbors == 2 or num_neighbors == 3):
                            new_grid[pos_str] = self.game_state["grid"][pos_str]
                        elif not is_alive and num_neighbors == 3:
                            if neighbor_owners:
                                most_common_owner = max(neighbor_owners, key=neighbor_owners.get)
                                new_grid[pos_str] = {"player": most_common_owner, "color": self.game_state["players"][most_common_owner]["color"]}
                    
                    self.game_state["grid"] = new_grid
                    self.game_state["tick_count"] += 1
                    
                    self.update_leaderboard()

                await self.broadcast_game_state()

                if time.time() - self.game_state["start_time"] >= self.game_state["game_duration"]:
                    await self.end_game_logic()
                    break

                elapsed_time = time.time() - start_tick_time
                await asyncio.sleep(max(0, self.game_state["tick_time"] / 1000.0 - elapsed_time))

            except asyncio.CancelledError:
                print("Game loop cancelled.")
                break
            except Exception as e:
                print(f"Error in game loop: {e}")
                break

    def update_leaderboard(self):
        """Updates the cell count for each player."""
        player_cells = {pid: 0 for pid in self.game_state["players"]}
        for cell in self.game_state["grid"].values():
            owner = cell.get("player")
            if owner in player_cells:
                player_cells[owner] += 1
        
        for pid, count in player_cells.items():
            if pid in self.game_state["players"]:
                self.game_state["players"][pid]["cells"] = count
                
    async def broadcast_game_state(self):
        """Sends the full game state to all clients."""
        if not self.game_state["players"]:
            return

        leaderboard = sorted(
            [{"username": p["username"], "color": p["color"], "percentage": p["cells"]} for p in self.game_state["players"].values()],
            key=lambda x: x["percentage"],
            reverse=True
        )
        
        remaining_time = max(0, self.game_state["game_duration"] - (time.time() - self.game_state["start_time"]))

        state_message = {
            "action": "update",
            "grid": self.game_state["grid"],
            "leaderboard": leaderboard,
            "info": {
                "remaining_time": int(remaining_time),
                "tick_count": self.game_state["tick_count"],
                "tick_time": self.game_state["tick_time"]
            }
        }
        await self.broadcast(json.dumps(state_message))

    async def broadcast_player_list(self):
        """Broadcasts the list of players in the lobby."""
        players = [{"username": p["username"], "color": p["color"]} for p in self.game_state["players"].values()]
        message = json.dumps({"action": "player_list_update", "players": players})
        await self.broadcast(message)

    async def broadcast_chat(self, player_id, message):
        """Broadcasts a chat message."""
        if player_id in self.game_state["players"]:
            player = self.game_state["players"][player_id]
            chat_message = {
                "action": "chat_message",
                "username": player["username"],
                "color": player["color"],
                "message": str(message)
            }
            await self.broadcast(json.dumps(chat_message))

    async def broadcast(self, message):
        """Sends a message to all connected clients."""
        if self.clients:
            for client_data in list(self.clients.values()):
                try:
                    await client_data["ws"].send(message)
                except websockets.exceptions.ConnectionClosed:
                    pass
    
    async def send_to_client(self, websocket, message):
        """Sends a message to a specific client."""
        try:
            await websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            pass

    async def send_error(self, websocket, error_message):
        """Sends an error message to a client."""
        await self.send_to_client(websocket, {"action": "error", "message": error_message})

    async def start_game_logic(self):
        """Logic for starting the game."""
        async with self.state_lock:
            if not self.game_state["players"]:
                print("No players in the lobby. Cannot start the game.")
                return
            if self.game_state["game_started"]:
                print("A game is already in progress.")
                return

            self.game_state["game_started"] = True
            self.game_state["grid"] = {}
            self.game_state["tick_count"] = 0
            
            await self.broadcast(json.dumps({"action": "start_game", "settings": {
                "width": self.game_state["width"],
                "height": self.game_state["height"]
            }}))
            
            self.game_loop_task = asyncio.create_task(self.game_loop())
            print("The game has started!")

    async def end_game_logic(self, forced=False):
        """Logic for ending the game."""
        async with self.state_lock:
            if not self.game_state["game_started"]:
                if forced: print("No game in progress.")
                return

            self.game_state["game_started"] = False
            if self.game_loop_task and not self.game_loop_task.done():
                self.game_loop_task.cancel()
            
            leaderboard = sorted(
                [{"username": p["username"], "color": p["color"], "percentage": p["cells"]} for p in self.game_state["players"].values()],
                key=lambda x: x["percentage"],
                reverse=True
            )

            await self.broadcast(json.dumps({"action": "end_game", "leaderboard": leaderboard[:3]}))
            print("The game is over.")
            self.reset_game_state()

    def reset_game_state(self):
        """Resets the game state for a new match."""
        self.game_state["grid"] = {}
        self.game_state["tick_count"] = 0
        self.game_state["start_time"] = None
        for player in self.game_state["players"].values():
            player["previews"] = []
            player["cells"] = 0

    async def handle_status_request(self, websocket):
        status_message = ""
        
        if self.game_state["game_started"]:
            remaining_time = self.game_state['game_duration'] - (time.time() - self.game_state['start_time'])
            players_count = len(self.game_state["players"])
            status_message = f"A game is in progress ({int(remaining_time)}s) ({players_count} player(s))"
        
        elif self.lobby_end_time:
            remaining_time = self.lobby_end_time - time.time()
            players_count = len(self.game_state["players"])
            if remaining_time > 0:
                status_message = f"A game will start in {int(remaining_time)}s with {players_count} player(s)"
        
        elif self.loop_running:
            status_message = f"Waiting for at least {self.min_players} player(s)"
            
        else:
            status_message = "Lobby not ready"

        await self.send_to_client(websocket, {
            "action": "status_update",
            "message": status_message
        })

    async def terminal(self):
        """Command-line interface to manage the server."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                command = await loop.run_in_executor(None, lambda: input("> "))
                parts = command.split()
                if not parts: continue
                cmd = parts[0].lower()

                if cmd == "startport":
                    if self.server:
                        print("Server is already running.")
                        continue
                    try:
                        port = int(parts[1]) if len(parts) > 1 else 5080
                        self.port = port
                        self.server = await websockets.serve(self.handle_client, "0.0.0.0", self.port)
                        print(f"Server started on port {self.port}.")
                    except (ValueError, IndexError):
                        print("Usage: startport <port>")
                    except Exception as e:
                        print(f"Error starting server: {e}")

                elif cmd == "set":
                    
                    if len(parts) < 3:
                        print("Usage: set <option> <value>")
                        print("Options: map, maxplayer, ticktime, time, waittime, minplayer")
                        continue
                    try:
                        setting, value = parts[1].lower(), parts[2]
                        if setting == "map":
                            width, height = map(int, value.split('x')) if 'x' in value else (int(value), int(parts[3]))
                            self.game_state["width"] = width
                            self.game_state["height"] = height
                            print(f"Map size set to {width}x{height}.")
                        elif setting == "maxplayer":
                            self.max_players = int(value)
                            print(f"Maximum players set to {'unlimited' if self.max_players == 0 else self.max_players}.")
                        elif setting == "ticktime":
                            self.game_state["tick_time"] = int(value)
                            print(f"Tick time set to {self.game_state['tick_time']}ms.")
                        elif setting == "time":
                            self.game_state["game_duration"] = int(value)
                            print(f"Game duration set to {self.game_state['game_duration']}s.")
                        elif setting == "waittime":
                            self.wait_time = int(value)
                            print(f"Lobby wait time set to {self.wait_time}s.")
                        elif setting == "minplayer":
                            self.min_players = int(value)
                            print(f"Minimum players to start set to {self.min_players}.")
                        else:
                            print("Unknown setting. Options: map, maxplayer, ticktime, time, waittime, minplayer")
                    except Exception as e:
                        print(f"Invalid command: {e}")
                
                elif cmd == "view":
                    if len(parts) < 2:
                        print("Usage: view <players|time|leaderboard|settings>")
                        continue
                    sub_cmd = parts[1].lower()
                    if sub_cmd == "players":
                        if not self.game_state["players"]:
                            print("No players connected.")
                        else:
                            for p in self.game_state["players"].values():
                                status = "In-Game" if self.game_state["game_started"] else "Waiting"
                                print(f"- {p['username']} ({status})")
                    elif sub_cmd == "time":
                        if self.game_state["game_started"] and self.game_state["start_time"]:
                            remaining = self.game_state['game_duration'] - (time.time() - self.game_state['start_time'])
                            print(f"Time remaining: {int(remaining//60):02d}:{int(remaining%60):02d}")
                        else:
                            print("The game has not started.")
                    elif sub_cmd == "leaderboard":
                        if not self.game_state["players"]:
                            print("No players.")
                        else:
                            sorted_players = sorted(self.game_state["players"].values(), key=lambda x:x.get('cells', 0), reverse=True)
                            for p in sorted_players:
                                print(f"- {p['username']}: {p.get('cells', 0)} cells")
                    elif sub_cmd == "settings":
                        print(f"- Minimum players: {self.min_players}")
                        print(f"- Lobby wait time: {self.wait_time}s")
                        print(f"- Max players: {'unlimited' if self.max_players == 0 else self.max_players}")
                        print(f"- Game duration: {self.game_state['game_duration']}s")

                elif cmd == "startgame":
                    await self.start_game_logic()
                
                elif cmd == "endgame":
                    await self.end_game_logic(forced=True)
                
                
                elif cmd == "startloop":
                    if self.loop_running:
                        print("The game loop is already running.")
                    else:
                        self.loop_running = True
                        self.main_loop_task = asyncio.create_task(self.main_loop())
                
                elif cmd == "endloop":
                    if not self.loop_running:
                        print("The game loop is not running.")
                    else:
                        self.loop_running = False
                        if self.main_loop_task and not self.main_loop_task.done():
                            self.main_loop_task.cancel()
                        if self.lobby_timer_task and not self.lobby_timer_task.done():
                            self.lobby_timer_task.cancel()
                        print("Game loop stopped. The current game (if any) will finish.")

                elif cmd == "kick":
                    if len(parts) < 2:
                        print("Usage: kick <username>")
                        continue
                    username_to_kick = parts[1]
                    player_ws_to_kick = next((ws for ws, data in self.clients.items() if data["id"].lower() == username_to_kick.lower()), None)
                    
                    if player_ws_to_kick:
                        player_id = self.clients[player_ws_to_kick]["id"]
                        await self.send_to_client(player_ws_to_kick, {"action": "kicked"})
                        await player_ws_to_kick.close()
                        print(f"Player {player_id} kicked.")
                    else:
                        print("Player not found.")
                
                elif cmd == "endport":
                    if self.server:
                        self.server.close()
                        await self.server.wait_closed()
                        self.server = None
                        print("Server stopped.")
                        break
                    else:
                        print("Server is not running.")

                else:
                    print("Unknown command. Available commands: startport, set, view, startgame, endgame, startloop, endloop, kick, endport")
            except (EOFError, KeyboardInterrupt):
                if self.server:
                    self.server.close()
                    await self.server.wait_closed()
                print("\nServer stopped.")
                break


if __name__ == "__main__":
    server = GameServer()
    try:
        asyncio.run(server.terminal())
    except KeyboardInterrupt:
        print("\nShutting down server.")