import asyncio
import random
import time
import uuid

import h5py
import numpy as np

from mango import RoleAgent


class CentralizedController(RoleAgent):
    def __init__(self, container, suggested_aid=None, agents=None):
        super().__init__(container, suggested_aid=suggested_aid)
        self.agents = agents
        self.new_created = False

    def handle_message(self, content, meta):
        self.malicious_agent_identified(content)

    async def store_sending_msg(self, content, m_id):
        current_time = time.time()
        self._hf = h5py.File(f'{self.aid}_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))

        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)

        general_group.create_dataset('time', data=np.float64(current_time))
        general_group.attrs["aid"] = self.aid
        self._hf.close()

    async def store_rcv_msg_to_db(self, content, m_id):
        current_time = time.time()
        self._hf = h5py.File(f'{self.aid}_rec_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))
        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)

        general_group.create_dataset('time', data=np.float64(current_time))
        general_group.attrs["aid"] = self.aid
        self._hf.close()

    def malicious_agent_identified(self, malicious_agent_id):
        # new communication topology without malicious agent
        if self.new_created:
            return
        self.new_created = True
        ctr = 0

        agent_neighbor_mapping = {}
        for agent_idx in range(int(len(self.agents))):
            if malicious_agent_id is not None:
                if self.agents[agent_idx].aid == malicious_agent_id:
                    pos = list(self.agents[agent_idx].flex.keys())[0]
                    flex = self.agents[agent_idx].flex[pos][-1]
                    if agent_idx == 0:
                        new_ = 1
                    else:
                        new_ = agent_idx - 1
                    self.agents[agent_idx].flagged_malicious = new_
                    try:
                        self.schedule_instant_acl_message(
                            content=flex, receiver_addr=self.addr,
                            receiver_id=self.agents[new_].aid,
                            acl_metadata={'sender_addr': self._context._container.addr,
                                          'sender_id': self.aid}, create_acl=True)

                    except Exception as e:
                        del self.agents[agent_idx]
                        self.new_created = False
                        self.malicious_agent_identified(None)
                        return
            ctr += 1
            if agent_idx % 2 != 0:
                continue

            if agent_idx == 0:
                if agent_idx not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx] = []

                agent_neighbor_mapping[agent_idx].append(self.agents[agent_idx + 1].aid)
                agent_neighbor_mapping[agent_idx].append(self.agents[-1].aid)

                if agent_idx+1 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx + 1] = []
                agent_neighbor_mapping[agent_idx + 1].append(self.agents[agent_idx].aid)

                if agent_idx-1 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx - 1] = []
                agent_neighbor_mapping[-1].append(self.agents[-1].aid)

            elif agent_idx == -1:
                if agent_idx-1 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx - 1] = []

                agent_neighbor_mapping[-1].append(self.agents[-2].aid)

                if agent_idx-2 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx - 2] = []

                agent_neighbor_mapping[-2].append(self.agents[-1].aid)
            else:

                if agent_idx not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx] = []
                agent_neighbor_mapping[agent_idx].append(self.agents[agent_idx + 1].aid)
                agent_neighbor_mapping[agent_idx].append(self.agents[agent_idx - 1].aid)

                if agent_idx+1 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx + 1] = []
                agent_neighbor_mapping[agent_idx + 1].append(self.agents[agent_idx].aid)

                if agent_idx-1 not in agent_neighbor_mapping.keys():
                    agent_neighbor_mapping[agent_idx - 1] = []
                agent_neighbor_mapping[agent_idx - 1].append(self.agents[agent_idx].aid)
        for agent_idx in range(len(self.agents)):
            self.schedule_instant_acl_message(
                content=agent_neighbor_mapping[agent_idx], receiver_addr=self.addr,
                receiver_id=self.agents[agent_idx].aid,
                acl_metadata={'sender_addr': self._context._container.addr,
                              'sender_id': self.aid}, create_acl=True)
            self.schedule_instant_task(self.store_sending_msg(agent_neighbor_mapping[agent_idx], m_id=uuid.uuid4()))



class DecentralizedController(RoleAgent):
    def __init__(self, container, neighbors, corresponding_agent, malicious_agent, controller=None):
        super().__init__(container)
        self.new_created = False
        self.neighbors = neighbors
        # tasks which should be triggered regularly
        self.tasks = []
        self.controller=controller
        task_settings = [
            (self.trigger_reaction),
        ]
        for trigger_fkt in task_settings:
            t = asyncio.create_task(trigger_fkt(corresponding_agent, malicious_agent))
            t.add_done_callback(self.raise_exceptions)
            self.tasks.append(t)

    async def store_sending_msg(self, content, m_id):
        current_time = time.time()
        self._hf = h5py.File(f'{self.aid}_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))

        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)

        general_group.create_dataset('time', data=np.float64(current_time))
        general_group.attrs["aid"] = self.aid
        self._hf.close()

    async def trigger_reaction(self, corresponding_agent, malicious_agent):
        duration = random.randint(20,60)
        await asyncio.sleep(duration)
        for neighbor in self.neighbors.keys():
            self.schedule_instant_acl_message(
                content=malicious_agent.aid, receiver_addr=self.neighbors[neighbor],
                receiver_id=neighbor,
                acl_metadata={'sender_addr': self._context._container.addr,
                              'sender_id': self.aid}, create_acl=True)
            self.schedule_instant_task(self.store_sending_msg(malicious_agent.aid, m_id=uuid.uuid4()))
        if self.controller:
            self.schedule_instant_acl_message(
                content=self.controller.aid, receiver_addr=self.addr,
                receiver_id=neighbor,
                acl_metadata={'sender_addr': self._context._container.addr,
                              'sender_id': self.aid}, create_acl=True)
            self.schedule_instant_task(self.store_sending_msg(malicious_agent.aid, m_id=uuid.uuid4()))
        pos = list(malicious_agent.flex.keys())[0]
        flex = malicious_agent.flex[pos][-1]
        malicious_agent.flagged_malicious = int(corresponding_agent.aid[-1])
        corresponding_agent.flex[list(corresponding_agent.flex.keys())[0]][-1] += flex