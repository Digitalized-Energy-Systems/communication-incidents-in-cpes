from typing import Dict, Any

import pandas as pd

from config import ID_AGENT_MAPPING, MANIPULATED_AGENT_ID
from mango import Role

from agents.decentralized_controller import DecentralizedController
from mango_library.coalition.core import CoalitionParticipantRole, CoalitionModel


class DecentralizedObserverRole(Role):

    def __init__(self, controller):
        super().__init__()
        self.detection_times = []
        self.csv_path = "centralized_observer_detection_times.csv"
        self.anomal_data_path = "anomalies.csv"
        self.controller = controller
        self.coalition_id = None

    def read_anomalies(self, coalition_id):
       #  anomalies = pd.read_csv(self.anomal_data_path)
        malicious_agents = ID_AGENT_MAPPING[MANIPULATED_AGENT_ID] # ['storage_agent_0']#anomalies['agent'].tolist()
        anomal_messages = 21# malicious_agents.count(malicious_agents[0])
        if anomal_messages > 10:
            self.detection_times.append(self._context._role_handler._agent_context._container.clock.time)
            pd.DataFrame(self.detection_times).to_csv(self.csv_path)
            for role in self._context._role_handler.roles:
                if isinstance(role, DecentralizedController):
                    role.exclude_malicious_agent(coalition_id, malicious_agents)

    def start_listening(self):
        coalition_id = None
        if self._context.aid == "storage_agent_0":
            for role in self._context._role_handler.roles:
                if isinstance(role, CoalitionParticipantRole):
                    coalition_id = list(self.context.get_or_create_model(CoalitionModel).assignments)[0]
            self.read_anomalies(coalition_id)

    def optimization_start(self):
        return