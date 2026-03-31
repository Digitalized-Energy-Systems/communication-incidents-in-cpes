import logging
import uuid

import h5py
import numpy as np

from agents.messages import Inactive
from config import CENTRALIZED_CONTROL, DECENTRALIZED_CONTROL, MULTI_LEVELLED
from mango import RoleAgent

GER = "%Y-%m-%d %H:%M:%S"
logger = logging.getLogger(__name__)


class AggregatorAgent(RoleAgent):

    def __init__(self, container, suggested_aid, controller, addrs=None):
        super().__init__(container, suggested_aid=suggested_aid)
        self.container = container
        self._hf = None
        self.exclusion_times = []
        self.csv_path = f'{self.aid}_exclusion.csv'
        self.controller = controller
        self.addrs = addrs

    def handle_message(self, content, meta):
        print('AGGR HANDLES; ', type(content))
        super().handle_message(content, meta)
        print(
            f"AggregatorAgent {self.aid} received a message with the following content: {type(content)}, at: {self.container.clock.time} from {meta['sender_addr']}")
        self.store_msg_to_db(content, meta['conversation_id'])

        if isinstance(content, Inactive):
            self.container.inactive = True

        if self.container._manipulation_number != np.inf:
            print('TIME LEFT?')
            print(self.addr, self.container._msg_counter, self.container._manipulation_number)
        if self.container._msg_counter >= self.container._manipulation_number and self.container._attack_scenario == 10 and CENTRALIZED_CONTROL:
            # timeout reached
            print('NO MESSAGES! INFORM CONTROLLER')
            self.schedule_instant_acl_message(Inactive(),
                                              receiver_addr=self.controller[0],
                                              receiver_id=self.controller[1],
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )
            self.container._manipulation_number = np.inf

        elif (
                self.container._msg_counter >= self.container._manipulation_number and self.container._attack_scenario == 10 and MULTI_LEVELLED) \
                or (
                self.container._msg_counter >= self.container._manipulation_number and self.container._attack_scenario == 10 and DECENTRALIZED_CONTROL):
            print('NO MESSAGES! INFORM OTHER AGENT', self.addrs)
            self.schedule_instant_acl_message(Inactive(additional_params={'agent_addrs': self.addrs}),
                                              receiver_addr=self.addrs[0][0],
                                              receiver_id=self.addrs[0][1],
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )
            self.container._manipulation_number = np.inf

    def store_msg_to_db(self, content, m_id):
        current_time = self.container.clock.time
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
