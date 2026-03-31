import random
import uuid

import h5py
import numpy as np
from mango import RoleAgent

from agents.messages import AggregatedSolutionMessage, CallForAdaption


class GridAgent(RoleAgent):

    def __init__(self, container, suggested_aid):
        super().__init__(container, suggested_aid)
        self._obligations = {}
        self.container = container
        self._hf = None

    def handle_message(self, content, meta):
        # This method defines what the agent will do with incoming messages.
        super().handle_message(content, meta)
        print(f"GridAgent ({self.aid}) received a message with the following content: {type(content)}")
        if isinstance(content, AggregatedSolutionMessage):
            self.handle_solution_message(content, meta)
        self.store_msg_to_db(content, meta['conversation_id'])

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

    def handle_solution_message(self, content, meta):
        probability = random.uniform(0, 1)
        # no obligation
        obligation_date = None
        obligation_value = None

        if probability >= 1.4:
            obligation_idx = random.randint(0, len(content.aggregated_flexibility) - 1)
            obligation_date = content.dates[obligation_idx]
            if obligation_date in self._obligations.keys():
                upper_margin = self._obligations[obligation_date] - 0.1
            else:
                upper_margin = content.aggregated_flexibility[obligation_idx][1] - 0.1
            obligation_value = round(random.uniform(content.aggregated_flexibility[obligation_idx][0],
                                                    upper_margin), 1)
            self._obligations[obligation_date] = obligation_value

        sender_addr = meta["sender_addr"]
        if isinstance(sender_addr, list):
            sender_addr = tuple(sender_addr)
        self.schedule_instant_acl_message(CallForAdaption(obligation_date=obligation_date,
                                                          obligation_value=obligation_value),
                                          receiver_addr=sender_addr,
                                          receiver_id=meta["sender_id"],
                                          acl_metadata={
                                              "sender_addr": self._context.addr,
                                              "sender_id": self.aid,
                                              "conversation_id": str(uuid.uuid4())
                                          }
                                          )
