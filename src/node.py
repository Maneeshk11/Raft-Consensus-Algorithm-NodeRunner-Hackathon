from __future__ import print_function
import asyncio
import random
import aiohttp
import requests
from flask import Flask, request, jsonify
import sys
import multiprocessing
import aiohttp
import tracemalloc
tracemalloc.start()

app = Flask(__name__)
app.app_context().push()


class RaftNode:
	def __init__(self, node_id, nodes):
		self.app = Flask(node_id)
		# self.app.add_url_rule('/requestVote', view_func=self.request_vote, methods=['POST', 'GET'])
		# self.app.add_url_rule('/appendEntries', view_func=self.send_heartbeats, methods=['POST', 'GET'])
		self.node_id = node_id
		self.nodes = nodes
		self.current_term = 0
		self.voted_for = None
		self.log = []
		self.commit_index = 0
		self.last_applied = 0
		self.next_index = {node_id: len(self.log) for node_id in self.nodes}
		self.match_index = {node_id: 0 for node_id in self.nodes}
		self.state = "follower"
		self.leader_id = None
		self.timeout = random.randint(5,15)
		self.votesGranted = 0
		self.votesReceived = 0
		self.ip = self.nodes[node_id]["ip"]
		self.port = self.nodes[node_id]["port"]

		
	async def run(self):
		print(self.timeout, file=sys.stdout)
		#flask_process = multiprocessing.Process(target=self.app.run, args=(self.ip, self.port))
		#flask_process.start()
		self.app.logger.info(self.timeout)
		while True:
			if self.state == "follower":
				# if self.node_id=="node2":
				# 	self.timeout=11
				# elif self.node_id=="node3":
				# 	self.timeout=13
				# else:
				# 	self.timeout=7
				await self.follower()
			elif self.state == "candidate":
				await self.candidate()
			elif self.state == "leader":
				await self.leader()
			#print(self.timeout, file=sys.stdout)
			self.app.logger.info(self.timeout)


	async def follower(self):
		self.reset_timeout()
		#self.timeout = 7
		await asyncio.sleep(self.timeout)
		print(self.timeout, file=sys.stdout)
		while True:
			if self.timeout <= 0:
				print(f"{self.node_id}: timed out as follower, the leader got killed", file=sys.stdout)
				self.state = "candidate"
				return
			await asyncio.sleep(1)
			self.timeout -= 1

	async def candidate(self):
		try:
			
			self.voted_for = self.node_id
			self.reset_timeout()
			self.votesReceived = 1
			vote_tasks = []
			for node_id in self.nodes:
				if node_id != self.node_id:
					votes = await self.request_vote(node_id)
					vote_tasks.append(votes)
			while self.state == "candidate":
				if self.timeout <= 0:
					self.reset_timeout()
					#self.current_term += 1
					self.votesReceived = 1
					vote_tasks = []
					for node_id in self.nodes:
						if node_id != self.node_id:
							votes = await self.request_vote(node_id)
							vote_tasks.append(votes)
				await asyncio.sleep(4)
				for vote_task in vote_tasks:
					try:
						response_data = vote_task
						response_data = response_data.data
						print(response_data, file=sys.stderr)
						# print(response_data, file=sys.stderr)
						# print(response_data, file=sys.stderr)
						if response_data[1]:
							self.votesReceived += 1
							if self.votesReceived > len(self.nodes) // 2:
								self.state = "leader"
								print(f"{self.node_id}: became leader", file=sys.stdout)
								return
						if response_data[0] > self.current_term:
							self.current_term = response_data['term']
							self.state = "follower"
							return
					except (aiohttp.ClientError, asyncio.TimeoutError) as e:
						print(f"Error requesting vote from {node_id}: {e}", file=sys.stdout)

		except Exception as e:
			print(e, file=sys.stdout)

	async def leader(self):
		self.current_term += 1
		print("OUR LEADER IS HERE", file=sys.stderr)
		#await asyncio.sleep(10)
		self.reset_timeout()
		while True:
			await asyncio.sleep(0.75)
			await self.send_heartbeats()
			#await self.appendEntries()
				

	def reset_timeout(self):
		self.timeout = random.randint(5,15)

	# async def appendEntries(self):
	# 	last_index = len(self.log) - 1
	# 	last_term = self.log[last_index]['term'] if self.log else 0
	# 	{
	# 		"term": self.current_term,
	# 		"leaderId": self.leader_id,
	# 		"prevLogIndex": last_index,
	# 		"prevLogTerm": last_term,
	# 		"entries": [{
	# 			"term": self.current_term,
	# 			"command": "command"
	# 		}],
	# 		"leaderCommit": self.last_applied
	# 	}
	#@app.route("/requestVote", methods = ["GET", "POST"])
	async def request_vote(self, node_id):
		try:
			"""
			Send a RequestVote RPC to the specified node to request its vote for becoming leader
			"""
			data= {
					"term": self.current_term,
					"voteGranted": True
					}
			
			print(data, file=sys.stdout)
			try:
				#node_id = "node2"
				url = f"http://{self.nodes[node_id]['ip']}:{self.nodes[node_id]['port']}/requestVote"
				print("url", file=sys.stderr)
				last_index = len(self.log) - 1
				last_term = self.log[last_index]['term'] if self.log else 0

				post_data = {
					'term': self.current_term,
					'candidateId': self.node_id,
					'last_LogIndex': last_index,
					'last_Log_Term': last_term,
				}
				print("requested_vote", file=sys.stderr)
				# response = ""
				# async with aiohttp.ClientSession() as session:
				# 	async with session.post(url, data=post_data) as response:
				response = requests.post(url, json=post_data, timeout=15)
				print(response.text, file=sys.stderr)

				# async with aiohttp.ClientSession() as session:
				# 	async with session.post(url, json=data) as response:
				if response.status_code == 200:
					response_data = response.json()
					if response_data['voteGranted']:
						self.votesReceived += 1
					if response_data['term'] > self.current_term:
						self.votesReceived -= 1
						self.current_term = response_data['term']
						self.state = "follower"
						
			except Exception as e:
				print(f"REQUEST VOTE {e}", file=sys.stdout)
		except Exception as e:
			print(f"REQUEST VOTE 2 {e}", file=sys.stdout)
		return jsonify(data)

	#@app.route("/appendEntries", methods = ["GET", "POST"])
	

	async def send_heartbeats(self): 
		try:
			data = {
				"term": self.current_term,
				"success": True
			}

			"""
			Send heartbeats to all nodes in the nodes to maintain leadership
			"""
			async with aiohttp.ClientSession() as session:
				for node_id in self.nodes:
					if node_id != self.node_id:  # Skip sending a heartbeat to self
						url = f"http://{self.nodes[node_id]['ip']}:{self.nodes[node_id]['port']}/appendEntries"
						last_index = len(self.log) - 1
						last_term = self.log[last_index]['term'] if self.log else 0

						post_data = {
							"term": self.current_term,
							"leaderId": node_id,
							"prevLogIndex": None,
							"prevLogTerm": None,
							"entries": [{
								"term": None,
								"command": None
							}],
							"leaderCommit": None
						}

						async with session.post(url, json=post_data) as response:
							print(f"Sent Heartbeat: {data}", file=sys.stderr)

							# Update the follower's term if it's behind
							if response.status == 200:
								response_data = await response.json()
								#print(f"Received Heartbeat {response_data}")
								if response_data['term'] > self.current_term:
									self.current_term = response_data['term']
									self.state = 'follower'
		except Exception as e:
			print(1111111111111111111111112222222222222222222222222222222)
			print(f"HEART HEART: {e}", file=sys.stderr)

		return data
	

	async def appendEntries(self, entries):
		try:
			acks = 0
			# entries = [{
			# 	"term": self.current_term,
			# 	"command": "SET JAKE"
			# }]
			entries[0]["term"] = self.current_term
			async with aiohttp.ClientSession() as session:
				
				for node_id in self.nodes:
					if node_id != self.node_id:
						url = f"http://{self.nodes[node_id]['ip']}:{self.nodes[node_id]['port']}/appendEntries"
						last_index = len(self.log) - 1
						last_term = self.log[last_index]['term'] if self.log else 0
						post_data = {
							"term": self.current_term,
							"leaderId": self.node_id,
							"prevLogIndex": last_index,
							"prevLogTerm": last_term,
							"entries": entries,
							"leaderCommit": self.last_applied
						}
						async with session.post(url, json=post_data) as response:
							print(f"Sent Log Data: {post_data}", file=sys.stderr)
							
							# Update the follower's term if it's behind
							if response.status == 200:
								response_data = await response.json()
								print(77777777777777777)
								print(response_data, file=sys.stderr)
								print(77777777777777777)
								if response_data["ack"]:
									acks+=1
								#print(f"Received Heartbeat {response_data}")
								# if response_data['term'] > self.current_term:
								# 	self.current_term = response_data['term']
								# 	self.state = 'follower'
				self.log.append(entries)
				if acks >= len(self.nodes) // 2:
					async with aiohttp.ClientSession() as session:
						for node_id in self.nodes:
							# if node_id != self.node_id:
							url = f"http://{self.nodes[node_id]['ip']}:{self.nodes[node_id]['port']}/handleLog"
							async with session.post(url, json={"code":"commit log"}) as response:
								if response.status == 200:
									pass
								else:
									print("error with saving data")	
				else:
					self.log.pop(len(self.log)-1)
		except Exception as e:
			print(f"LOG ERROR: {e}", file=sys.stderr)	
					


async def main():
	# Define the nodes
	nodes = {
		"node1": {"ip": "127.0.0.1", "port": 8000},
		"node2": {"ip": "127.0.0.1", "port": 8001},
	}

	# # Create and start the nodes
	# tasks = []
	# for node_id in nodes:
	# 	node = RaftNode(node_id, nodes)
	# 	tasks.append(asyncio.create_task(node.run()))

	# # Run the nodes' event loops
	# await asyncio.gather(*tasks)
	node_id = "node1"
	#app.run(nodes[node_id]["ip"], port=nodes[node_id]["port"])
	node = RaftNode(node_id, nodes)
	#app.run(nodes[node_id]["ip"], port=nodes[node_id]["port"])
	await node.run()


if __name__ == '__main__':
	asyncio.run(main())