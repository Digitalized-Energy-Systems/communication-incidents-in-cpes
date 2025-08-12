import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta

import h5py
import numpy as np
from mango import RoleAgent
from mango_library.coalition.core import CoalitionAssignmentConfirm
from mango_library.negotiation.cohda.cohda_messages import StartCohdaNegotiationMessage, ConfirmCohdaSolutionMessage
from mango_library.negotiation.termination import NegotiationTerminationDetectorRole, TerminationMessage

from agents.messages import AggregatedSolutionMessage, RedispatchFlexibilityRequest, RedispatchFlexibilityReply, \
    CallForAdaption
from config import SIMULATION_HOURS_IN_RESOLUTION, END

GER = "%Y-%m-%d %H:%M:%S"
logger = logging.getLogger(__name__)


class AggregatorAgent(RoleAgent):

    def __init__(self, container, n_agents, suggested_aid, target_params, negotiation_timeout):
        super().__init__(container, suggested_aid=suggested_aid)
        self._n_agents = n_agents
        self._final_solution = None
        self._coalition_confirms = 0
        self._coalition_id = None
        self._agent_addrs = None
        self._grid_agent_addr = None
        self._current_date_time = target_params['current_start_date']
        self._current_date_time_obj = datetime.strptime(target_params['current_start_date'], GER)
        self._end_date = datetime.strptime(END, GER)
        self._open_confirmations = {}
        self._confirmed_cohda_solutions = []
        self._redispatch_flexibilities = {}
        self._aggregated_flexibility = {}
        self._obligations = []
        self.handled_solutions = []
        self._current_dates = []
        self.container = container
        self._hf = None
        start_dt = self._current_date_time_obj
        for idx in range(SIMULATION_HOURS_IN_RESOLUTION):
            new_date_str = start_dt.strftime(GER)
            self._current_dates.append(new_date_str)
            start_dt = start_dt + timedelta(minutes=15)

        self._adaption_running = False
        self.db_file = 'results' + self._current_date_time + '.hdf5'
        with h5py.File(self.db_file, "w") as f:
            f.close()
        self.negotiation_start_time = 0
        self.negotiation_end_time = 0
        self.step_done = asyncio.Future()
        self._agent_names = []
        self._cohda_agents = []
        self._after_adaption = False
        self._negotiation_timeout = negotiation_timeout
        self._seconds_running = 0.
        self._c_neg_id = None
        self._negotiation_running = False

    def handle_message(self, content, meta):
        super().handle_message(content, meta)
        print(
            f"AggregatorAgent {self.aid} received a message with the following content: {type(content)}, at: {self.container.clock.time}")
        if isinstance(content, CoalitionAssignmentConfirm):
            self.handle_coalition_confirm(content, meta)
        if isinstance(content, ConfirmCohdaSolutionMessage):
            self.handle_solution_confirm(content, meta)
        if isinstance(content, RedispatchFlexibilityReply):
            self.handle_redispatch_flexibilty(content, meta)
        if isinstance(content, CallForAdaption):
            self.handle_call_for_adaption(content, meta)
        if isinstance(content, TerminationMessage):
            print(meta['sender_id'])
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

    async def _update_obligations(self, obligation_date, obligation_value):
        updated_obligations = []
        if len(self._obligations) > 1:
            for obligation in self._obligations:
                if obligation[0] == obligation_date:
                    updated_obligations.append((obligation_date, obligation_value))
                    continue
                elif obligation[0] >= self._current_date_time_obj:
                    updated_obligations.append(obligation)
        else:
            updated_obligations.append((obligation_date, obligation_value))
        self._obligations = updated_obligations

    def handle_call_for_adaption(self, content: CallForAdaption, meta):
        if not content.obligation_date:
            # no adaption necessary
            self._after_adaption = False
            self.check_next_step(after_adaption=False, store_results=False)
            return
        del self.roles[-1]
        self.add_role(NegotiationTerminationDetectorRole())
        self.schedule_instant_task(self._update_obligations(content.obligation_date, content.obligation_value))
        for agent in self.agent_addrs:
            agent_addr = agent[0]
            agent_id = agent[1]
            self._adaption_running = True
            self.schedule_instant_acl_message(StartCohdaNegotiationMessage(coalition_id=self._coalition_id,
                                                                           send_weight=True,
                                                                           target_params={
                                                                               'obligations': self._obligations,
                                                                               'current_start_date': self._current_date_time}),
                                              receiver_addr=agent_addr,
                                              receiver_id=agent_id,
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )
        self.negotiation_start_time = self.container.clock.time

    def handle_redispatch_flexibilty(self, content: RedispatchFlexibilityReply, meta):
        sender_addr = meta["sender_addr"]
        if isinstance(sender_addr, list):
            sender_addr = tuple(sender_addr)
        self._redispatch_flexibilities[content.dates[0]][meta["sender_id"]] = content.flexibility
        if len(self._aggregated_flexibility) == 0:
            self._aggregated_flexibility = [[0, 0] for _ in range(len(content.flexibility))]

        for idx in range(len(content.flexibility)):
            try:
                self._aggregated_flexibility[idx][0] += content.flexibility[idx][0]
                self._aggregated_flexibility[idx][1] += content.flexibility[idx][1]
            except TypeError as e:
                self._aggregated_flexibility[idx][0] += content.flexibility[idx]
                self._aggregated_flexibility[idx][1] += content.flexibility[idx]

        if len(self._redispatch_flexibilities[content._dates[0]]) == self._n_agents:
            self._after_adaption = False
            self.store_results(after_adaption=False)
            self.schedule_instant_acl_message(AggregatedSolutionMessage(aggregated_solution=self._final_solution,
                                                                        aggregated_flexibility=self._aggregated_flexibility,
                                                                        dates=self._current_dates),
                                              receiver_addr=self.grid_agent_addr[0],
                                              receiver_id=self.grid_agent_addr[1],
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )

    async def store_final_msgs(self):
        f = h5py.File(self.db_file, 'a')
        grp_name = f"negotiation+{str(self._current_date_time)}+after_adaption:{self._after_adaption}"
        if grp_name not in f:
            print('try to store msgs but does not exist?')
            grp = f.create_group(grp_name)
        else:
            grp = f[grp_name]
        if os.path.isfile("aggregator_agent_rec_msg.h5"):
            # open updates from agents, if those are stored
            agent_msgs = [f"{agent_name}_rec_msg.h5" for agent_name in self._agent_names]
            for c_agent_name in agent_msgs:
                file = h5py.File(c_agent_name, "a")
                if os.path.isfile(c_agent_name):
                    groups = [key for key in file.keys()]
                    groups.sort()
                    for key in groups:
                        file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
                # os.remove(c_agent_name)
        if os.path.isfile("aggregator_agent_msg.h5") or os.path.isfile("generation_agent_0_msg.h5") or os.path.isfile(
                "storage_agent_0_msg.h5"):
            # open updates from agents, if those are stored
            agent_msgs = [f"{agent_name}_msg.h5" for agent_name in self._agent_names]
            for c_agent_name in agent_msgs:
                file = h5py.File(c_agent_name, "a")
                if os.path.isfile(c_agent_name):
                    groups = [key for key in file.keys()]
                    groups.sort()
                    for key in groups:
                        file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
                # os.remove(c_agent_name)
        if os.path.isfile("generation_agent_0.h5"):
            # open updates from agents, if those are stored
            agent_msgs = [f"{agent_name}.h5" for agent_name in self._cohda_agents]
            for c_agent_name in agent_msgs:
                file = h5py.File(c_agent_name, "a")
                if os.path.isfile(c_agent_name):
                    groups = [key for key in file.keys()]
                    groups.sort()
                    for key in groups:
                        file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
                # os.remove(c_agent_name)
        f.close()

    def store_results(self, after_adaption):
        f = h5py.File(self.db_file, 'a')
        grp_name = f"negotiation+{str(self._current_date_time)}+after_adaption:{after_adaption}"
        if grp_name in f:
            # already stored
            print('already stored?')
            return
        grp = f.create_group(grp_name)
        if os.path.isfile(f"{self.cohda_agents[0]}.h5"):
            # open updates from agents, if those are stored
            agent_updates = [f"{agent_name}.h5" for agent_name in self.cohda_agents]
            for agent_name in agent_updates:
                file = h5py.File(agent_name, "a")
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{agent_name[:-2]}_{key}")
            # os.remove(agent_name)
        if os.path.isfile("aggregator_agent_rec_msg.h5"):
            # open updates from agents, if those are stored
            agent_msgs = [f"{agent_name}_rec_msg.h5" for agent_name in self._agent_names]

            for c_agent_name in agent_msgs:
                file = h5py.File(c_agent_name, "a")
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            # os.remove(c_agent_name)

        if os.path.isfile("aggregator_agent_msg.h5"):
            # open updates from agents, if those are stored
            agent_msgs_sent = [f"{agent_name}_msg.h5" for agent_name in self._agent_names]

            for c_agent_name in agent_msgs_sent:
                file = h5py.File(c_agent_name, "a")
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
                # os.remove(c_agent_name)
        obligations = [[datetime.strptime(a[0], GER).timestamp(), a[1]] for a in self._obligations]
        grp.create_dataset("obligations", data=obligations)
        if len(self._aggregated_flexibility) > 1:
            grp.create_dataset("aggregated_flexibility", data=self._aggregated_flexibility)
            grp.create_dataset("perf", data=self._final_solution.perf)
            grp.create_dataset("solution_entries", data=list(self._final_solution.schedules.values()))
            choices = list(self._final_solution.schedules.keys())
            choices = [int(entry) for entry in choices]
            grp.create_dataset("solution_choices", data=choices)
            grp.create_dataset("duration", data=self.negotiation_end_time - self.negotiation_start_time)
            grp.create_dataset("start", data=self._current_date_time_obj.timestamp())
        f.close()

    def check_next_step(self, after_adaption, store_results=True):
        self._negotiation_running = False
        if store_results:
            self.store_results(after_adaption)
        start_dt = self._current_date_time_obj + timedelta(minutes=15)
        if start_dt <= self._end_date:
            self._current_date_time = datetime.strftime(start_dt, GER)
            self._current_date_time_obj = start_dt
            self.db_file = 'results' + self._current_date_time + '.hdf5'
            del self._current_dates[0]
            new_date = datetime.strptime(self._current_dates[-1], GER) + timedelta(minutes=15)
            self._current_dates.append(datetime.strftime(new_date, GER))
            self._aggregated_flexibility = {}
            self._final_solution = None
            self._adaption_running = False
            with h5py.File(self.db_file, "w") as f:
                f.close()
            self.start_negotiation()
        else:
            self.step_done.set_result(True)

    def handle_coalition_confirm(self, content: CoalitionAssignmentConfirm, meta):
        """
        Handles COHDA solution
        Saves solution, aggregates solution and forwards it to grid agent
        """
        if self._coalition_id is None:
            self._coalition_id = content.coalition_id
        self._coalition_confirms += 1
        if self._coalition_confirms == self._n_agents:
            # addresses are tuples from (container_addr, agent_id)
            agent_addr = self.agent_addrs[0][0]
            agent_id = self._agent_addrs[0][1]
            self.schedule_instant_acl_message(StartCohdaNegotiationMessage(coalition_id=content.coalition_id,
                                                                           send_weight=True),
                                              receiver_addr=agent_addr,
                                              receiver_id=agent_id,
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )
            self.negotiation_start_time = self.container.clock.time
            self._negotiation_running = True
            # self.schedule_conditional_task(self.check_next_step,
            #                                condition_func=self._is_next_step)
            # self.schedule_conditional_task(self.cancel_negotiation(), condition_func=self._check_timeout)

    async def cancel_negotiation(self):
        print('cancel neg', self._c_neg_id)
        self._negotiation_running = False
        if self._c_neg_id is not None:
            if self._c_neg_id in self._open_confirmations:
                if self._c_neg_id not in self.handled_solutions:
                    self.handled_solutions.append(self._c_neg_id)
                    if self._adaption_running:
                        # flexibility already received
                        self._adaption_running = False
                        self._after_adaption = True
                        self.check_next_step(after_adaption=True, store_results=True)
                        return
                    del self._open_confirmations[self._c_neg_id]
                    self._confirmed_cohda_solutions.append(self._c_neg_id)
                    self._redispatch_flexibilities[self._current_date_time] = {}
                    for agent in self._agent_addrs:
                        self.schedule_instant_acl_message(
                            RedispatchFlexibilityRequest(dates=self._current_dates, obligations=self._obligations),
                            receiver_addr=agent[0], receiver_id=agent[1],
                            acl_metadata={"sender_addr": self._context.addr,
                                          "sender_id": self.aid, "conversation_id": str(uuid.uuid4())
                                          })
                    return
        self.check_next_step(after_adaption=self._after_adaption)

    def handle_solution_confirm(self, content: ConfirmCohdaSolutionMessage, meta):
        neg_id = content.negotiation_id
        self.negotiation_end_time = self.container.clock.time
        self._final_solution = content.final_candidate

        if neg_id not in self._open_confirmations.keys():
            self._open_confirmations[neg_id] = self._n_agents
        self._open_confirmations[neg_id] -= 1
        # all confirmations received
        if self._open_confirmations[neg_id] == 0:
            self._c_neg_id = None
            if neg_id in self.handled_solutions:
                return
            self.handled_solutions.append(neg_id)
            if self._adaption_running:
                # flexibility already received
                self._adaption_running = False
                self._after_adaption = True
                self._negotiation_running = False
                self.check_next_step(after_adaption=True, store_results=True)
                return
            del self._open_confirmations[neg_id]
            self._confirmed_cohda_solutions.append(neg_id)
            self._redispatch_flexibilities[self._current_date_time] = {}
            for agent in self._agent_addrs:
                self.schedule_instant_acl_message(
                    RedispatchFlexibilityRequest(dates=self._current_dates, obligations=self._obligations),
                    receiver_addr=agent[0], receiver_id=agent[1],
                    acl_metadata={"sender_addr": self._context.addr,
                                  "sender_id": self.aid, "conversation_id": str(uuid.uuid4())
                                  })

    def start_negotiation(self):
        del self.roles[-1]
        self._negotiation_running = True
        # all 5 minutes
        self._seconds_running += 900.
        #self.container.clock.set_time(self._seconds_running)
        self.add_role(NegotiationTerminationDetectorRole())
        agent_addr = self.agent_addrs[0][0]
        agent_id = self._agent_addrs[0][1]
        self.schedule_instant_acl_message(StartCohdaNegotiationMessage(coalition_id=self._coalition_id,
                                                                       send_weight=True,
                                                                       target_params={
                                                                           'obligations': self._obligations,
                                                                           'current_start_date': self._current_date_time,
                                                                       }
                                                                       ),
                                          receiver_addr=agent_addr,
                                          receiver_id=agent_id,
                                          acl_metadata={
                                              "sender_addr": self._context.addr,
                                              "sender_id": self.aid,
                                              "conversation_id": str(uuid.uuid4())
                                          }
                                          )
        self.negotiation_start_time = self.container.clock.time
        # self.schedule_conditional_task(self.cancel_negotiation(), condition_func=self._check_timeout)

    @property
    def agent_addrs(self):
        return self._agent_addrs

    @agent_addrs.setter
    def agent_addrs(self, agent_addrs):
        self._agent_addrs = agent_addrs

    @property
    def grid_agent_addr(self):
        return self._grid_agent_addr

    @grid_agent_addr.setter
    def grid_agent_addr(self, grid_agent_addr):
        self._grid_agent_addr = grid_agent_addr

    @property
    def agent_names(self):
        return self._agent_names

    @agent_names.setter
    def agent_names(self, agent_names):
        self._agent_names = agent_names

    @property
    def cohda_agents(self):
        return self._cohda_agents

    @cohda_agents.setter
    def cohda_agents(self, cohda_agents):
        self._cohda_agents = cohda_agents

    def _check_timeout(self):
        print('check if timeout reached', self.container.clock.time,
              self.container.clock.time - self.negotiation_start_time)
        timeout_reached = (self.container.clock.time - self.negotiation_start_time) >= self._negotiation_timeout
        print(timeout_reached)
        return timeout_reached

    def _is_next_step(self):
        return self.container.clock.time >= self._seconds_running + 900.
