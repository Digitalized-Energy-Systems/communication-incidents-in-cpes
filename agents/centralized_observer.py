import asyncio
from typing import Dict, Any

import pandas as pd

from config import ID_AGENT_MAPPING, MANIPULATED_AGENT_ID
from mango import Role

class CentralizedObserverAgentRole(Role):

    def __init__(self, controller, agents=None):
        super().__init__()
        self.detection_times = []
        self.csv_path = "centralized_observer_detection_times.csv"
        self.anomal_data_path = "agents/anomalies.csv"
        self.controller = controller
        self.sending_status = {}
        self.aid_agent_mapping = {}
        self.conspicuous_agents = []
        self.tasks = []
        self.to_check = []

    def handle_message(self, content, meta: Dict[str, Any]):
        pass

    def agents(self, agents):
        for agent in agents:
            self.sending_status[agent] = []

    def read_anomalies(self):
        # centralized
        anomalies = pd.read_csv(self.anomal_data_path)
        malicious_agents = anomalies['agent'].tolist()
        anomal_messages = malicious_agents.count(malicious_agents[0])
        if anomal_messages > 10:
            self.detection_times.append(self._context._role_handler._agent_context._container.clock.time)
            pd.DataFrame(self.detection_times).to_csv(self.csv_path)
            self.trigger_controller(ID_AGENT_MAPPING[MANIPULATED_AGENT_ID])


    def trigger_controller(self, malicious_agent):
        self.controller.anomaly_found(malicious_agent)

    def start_listening(self):
        self.read_anomalies()

    async def check_for_conspicious_agents(self):
        await asyncio.sleep(5)
        for agent in self.to_check:
            if self.sending_status[agent]:
                if agent in self.conspicuous_agents:
                    # trigger control
                    self.controller.malicious_agent_identified(agent)
                else:
                    self.conspicuous_agents.append(agent)
        self.to_check = []

    def msg_send(self, sender, receiver=[]):
        for agent in receiver:
            self.sending_status[agent] = True
            self.to_check.extend(receiver)

        if sender in self.sending_status.keys():
            self.sending_status[sender] = False

        task_settings = [
            (self.check_for_conspicious_agents),
        ]
        for trigger_fkt in task_settings:
            t = asyncio.create_task(trigger_fkt())
            self.tasks.append(t)
