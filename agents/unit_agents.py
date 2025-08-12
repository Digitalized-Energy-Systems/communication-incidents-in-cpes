import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from typing import List

import h5py
import numpy as np
import pandas as pd
from mango import RoleAgent
from midas.util.base_data_model import DataModel

from agents.flexibility_model import AdaptedFlexibilityModel
from agents.messages import RedispatchFlexibilityRequest, RedispatchFlexibilityReply
from config import MIDAS_DATA, STEP_SIZE, PARAMS_BATT, INITS_BATT, SIMULATION_HOURS_IN_RESOLUTION, WD_PATH, T_AIR, WIND, \
    PRESSURE, SCHEDULE_PERCENTAGES, BH, DH, SIMULATION_HOURS, NUMBER_OF_SCHEDULES_PER_AGENT
from mango_library.negotiation.cohda.cohda_messages import StartCohdaNegotiationMessage
from pysimmods.buffer.batterysim import Battery
from pysimmods.generator import WindPowerPlantSystem
from pysimmods.generator.chplpgsim import CHPLPG
from pysimmods.generator.chplpgsystemsim.presets import chp_preset
from pysimmods.generator.pvsystemsim import PVPlantSystem
from pysimmods.generator.pvsystemsim.presets import pv_preset
from pysimmods.generator.windsystemsim.presets import wind_presets
from pysimmods.util.date_util import GER
from util import datetime_to_index


class UnitAgent(RoleAgent):
    def __init__(self, container, suggested_aid, schedule_provider=None):
        super().__init__(container, suggested_aid)
        self._flex_for_date = {}
        self._current_dates = ['0']
        self._schedules_for_date = {}
        self._obligations = {}
        self._hf = None
        self.container = container

    def update_dates(self, start_date):
        self._current_dates = []
        start_date = start_date + 'Z'
        start_dt = datetime.strptime(start_date, GER)
        for idx in range(SIMULATION_HOURS_IN_RESOLUTION):
            new_date = start_dt.strftime(GER)
            new_date = new_date[0:len(new_date) - 5]
            self._current_dates.append(new_date)

    def calculate_penalty(self, target_params, chosen_schedule):
        return 0

    def calculate_redispatch_flexibility(self, start_date=None) -> List:
        return 0

    def store_msg_to_db(self, content, m_id):
        current_time = self.container.clock.time
        self._hf = h5py.File(f'{self.aid}_rec_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))
        general_group.create_dataset('time', data=np.float64(current_time))
        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)
        general_group.attrs["aid"] = self.aid
        self._hf.close()

    def handle_message(self, content, meta):
        # This method defines what the agent will do with incoming messages.
        print(
            f"UnitAgent {self.aid} received a message with the following content: {type(content)} at {self.container.clock.time}")
        if isinstance(content, RedispatchFlexibilityRequest):
            if content.dates[0] not in self._flex_for_date.keys():
                self.calculate_redispatch_flexibility(content.dates[0])
            sender_addr = meta["sender_addr"]
            if isinstance(sender_addr, list):
                sender_addr = tuple(sender_addr)
            self.schedule_instant_acl_message(RedispatchFlexibilityReply(dates=content.dates,
                                                                         flexibility=self._flex_for_date[
                                                                             content.dates[0]]),
                                              receiver_addr=sender_addr,
                                              receiver_id=meta["sender_id"],
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )
        if isinstance(content, StartCohdaNegotiationMessage):
            if content.target_params:
                if 'obligations' in content.target_params.keys():
                    for entry in content.target_params['obligations']:
                        self._obligations[entry[0]] = entry[1]
        super().handle_message(content, meta)
        self.store_msg_to_db(content, meta['conversation_id'])


class LoadAgent(UnitAgent):

    def __init__(self, container, suggested_aid, schedule_provider=None, scaling=1.0):
        super().__init__(container, suggested_aid)
        if schedule_provider is None:
            self.schedule_provider = self.schedule_provider_no_flexibility
        else:
            self.schedule_provider = schedule_provider
        self._power_forecast = []
        self._power_forecast_per_date = {}
        self._scaling = scaling

    def get_power_forecast(self, start_date, scaling=1.0, **unit_parameters) -> List:
        if start_date is not None and start_date != self._current_dates[0]:
            self.update_dates(start_date)
        power_forecast = []
        start = start_date + 'Z'
        load_p = pd.read_hdf(MIDAS_DATA, "load_pmw")[0]
        try:
            data_q = pd.read_hdf(MIDAS_DATA, "load_qmvar")[0]
        except KeyError:
            # No q values for loads available. Skipping.
            data_q = None
        idx = 0
        # col = load_p[idx]
        rng = np.random.RandomState()
        model = DataModel(
            data_p=load_p,
            data_q=data_q,
            data_step_size=900,
            scaling=scaling,
            seed=rng.randint(1_000_000)
        )
        model.cos_phi = 0.9
        start_dt = datetime.strptime(start, GER)
        for _ in range(SIMULATION_HOURS_IN_RESOLUTION):
            model.now_dt = start_dt
            model.step()
            power_forecast.append(-model.p_mw)
            start_dt = start_dt + timedelta(minutes=15)
        return [power_forecast]

    def schedule_provider_no_flexibility(self, target_params):
        if target_params['current_start_date'] != self._current_dates[0]:
            self.update_dates(target_params['current_start_date'])
        if target_params['current_start_date'] not in self._power_forecast_per_date.keys():
            fc = self.get_power_forecast(
                start_date=target_params['current_start_date'],
                forecast_length=SIMULATION_HOURS
            )
            self._power_forecast_per_date[target_params['current_start_date']] = fc
        else:
            fc = self._power_forecast_per_date[target_params['current_start_date']]
        return fc

    def calculate_redispatch_flexibility(self, start_date=None) -> List:
        if start_date is not None and start_date != self._current_dates[0]:
            self.update_dates(start_date)
        if start_date is not None and start_date in self._power_forecast_per_date.keys():
            self._power_forecast = self._power_forecast_per_date[start_date][0]
        else:
            fc = self.get_power_forecast(start_date=start_date, forecast_length=SIMULATION_HOURS, scaling=self._scaling)
            self._power_forecast = fc[0]
        flex = []
        for idx in range(len(self._power_forecast)):
            flex.append([self._power_forecast[idx], self._power_forecast[idx]])
        self._flex_for_date[start_date] = flex
        return flex


class CHPAgent(UnitAgent):

    def __init__(self, container, suggested_aid, schedule_provider=None, kw=None):
        super().__init__(container, suggested_aid)
        if schedule_provider is None:
            self.schedule_provider = self.schedule_provider_chp
        else:
            self.schedule_provider = schedule_provider
        if kw is None:
            kw = 400
        chp_params, chp_inits = chp_preset(kw)
        self._chp_model = CHPLPG(params=chp_params['chp'], inits=chp_inits['chp'])
        self._schedule_model = AdaptedFlexibilityModel(self._chp_model, step_size=STEP_SIZE)
        self._schedules_per_date = {}

        def reset() -> None:
            """To be called at the end of each step."""
            for attr in self._chp_model.inputs.__dict__.keys():
                if attr == 'e_th_demand_set_kwh':
                    setattr(self, attr, 0)
                else:
                    setattr(self, attr, None)

        self._chp_model.inputs.reset = reset

    def schedule_provider_chp(self, target_params):
        if target_params['current_start_date'] != self._current_dates[0]:
            self.update_dates(target_params['current_start_date'])
        if target_params['current_start_date'] in self._schedules_per_date.keys():
            return self._schedules_per_date[target_params['current_start_date']]
        start = target_params['current_start_date'] + 'Z'

        self._schedule_model.set_now_dt(start)
        self._schedule_model.set_step_size(STEP_SIZE)
        self._chp_model.set_now_dt(start)
        self._chp_model.inputs.e_th_demand_set_kwh = 0
        self._schedule_model.inputs.e_th_demand_set_kwh = 0
        self._schedule_model.step()
        schedules = self._schedule_model.generate_schedules(start,
                                                            flexibility_horizon_hours=SIMULATION_HOURS,
                                                            num_schedules=NUMBER_OF_SCHEDULES_PER_AGENT)
        correct_schedules = []
        for entry in schedules._schedules.values():
            entry = list(entry.to_dict()['p_kw'].values())
            # kw to mw
            entry = [-x / 1000 for x in entry]
            if self._obligations is not None:
                for s_idx, date in enumerate(self._current_dates):
                    if date in self._obligations.keys():
                        entry[s_idx] = self._obligations[date]
            correct_schedules.append(entry)

        self._schedules_per_date[target_params['current_start_date']] = correct_schedules
        return correct_schedules

    def calculate_redispatch_flexibility(self, start_date='') -> List:
        if start_date is not None and start_date != self._current_dates[0]:
            self.update_dates(start_date)
        if start_date is not None and start_date in self._flex_for_date.keys():
            return self._flex_for_date[start_date]
        start = start_date + 'Z'
        copy_model = deepcopy(self._schedule_model)
        copy_model.set_now_dt(start)
        copy_model.set_step_size(STEP_SIZE)

        copy_model.step()
        flex_max = copy_model.maximum_flex(start,
                                           flexibility_horizon_hours=SIMULATION_HOURS)
        flex_max = list(flex_max.to_dict()['p_kw'].values())

        copy_model = deepcopy(self._schedule_model)
        copy_model.set_now_dt(start)
        copy_model.set_step_size(STEP_SIZE)

        copy_model.step()
        flex_min = copy_model.minimum_flex(start,
                                           flexibility_horizon_hours=SIMULATION_HOURS)
        flex_min = list(flex_min.to_dict()['p_kw'].values())
        start_dt = datetime.strptime(start, GER)

        flex = []
        for idx in range(len(flex_max)):
            # kw to mw
            flex.append([-flex_min[0] / 1000, -flex_max[0] / 1000])
            start_dt = start_dt + timedelta(minutes=15)
        if self._obligations is not None:
            for s_idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    flex[s_idx] = self._obligations[date]
        self._flex_for_date[start_date] = flex
        return flex


class BatteryAgent(UnitAgent):

    def __init__(self, container, suggested_aid, schedule_provider=None, cap_kwh=None):
        super().__init__(container, suggested_aid)
        if schedule_provider is None:
            self.schedule_provider = self.schedule_provider
        else:
            self.schedule_provider = schedule_provider
        if cap_kwh is not None:
            PARAMS_BATT['cap_kwh'] = cap_kwh
        self._battery_model = Battery(params=PARAMS_BATT, inits=INITS_BATT)
        self._battery_model.inputs.step_size = STEP_SIZE
        self._schedule_model = AdaptedFlexibilityModel(self._battery_model, step_size=STEP_SIZE)

    def calculate_penalty(self, target_params, chosen_schedule):
        return 0

    def schedule_provider(self, target_params):
        if target_params['current_start_date'] != self._current_dates[0]:
            self.update_dates(target_params['current_start_date'])
        if target_params['current_start_date'] in self._schedules_for_date.keys():
            return self._schedules_for_date[target_params['current_start_date']]
        start = target_params['current_start_date'] + 'Z'
        self._schedule_model.set_now_dt(start)
        self._schedule_model.set_step_size(STEP_SIZE)

        self._schedule_model.step()
        schedules = self._schedule_model.generate_schedules(start,
                                                            flexibility_horizon_hours=SIMULATION_HOURS,
                                                            num_schedules=NUMBER_OF_SCHEDULES_PER_AGENT)
        correct_schedules = []
        for entry in schedules._schedules.values():
            entry = list(entry.to_dict()['p_kw'].values())
            # kw to mw
            entry = [-x / 1000 if x != 0 else -x for x in entry]
            if self._obligations is not None:
                for s_idx, date in enumerate(self._current_dates):
                    if date in self._obligations.keys():
                        entry[s_idx] = self._obligations[date]
            correct_schedules.append(entry)
        self._schedules_for_date[target_params['current_start_date']] = correct_schedules
        return correct_schedules

    def calculate_redispatch_flexibility(self, start_date='') -> List:
        if start_date is not None and start_date != self._current_dates[0]:
            self.update_dates(start_date)
        if start_date is not None and start_date in self._flex_for_date.keys():
            return self._flex_for_date[start_date]
        start = start_date + 'Z'
        copy_model = deepcopy(self._schedule_model)
        copy_model.set_now_dt(start)
        copy_model.set_step_size(STEP_SIZE)

        copy_model.step()
        flex_max = copy_model.maximum_flex(start,
                                           flexibility_horizon_hours=SIMULATION_HOURS)
        flex_max = list(flex_max.to_dict()['p_kw'].values())

        copy_model = deepcopy(self._schedule_model)
        copy_model.set_now_dt(start)
        copy_model.set_step_size(STEP_SIZE)

        copy_model.step()
        flex_min = copy_model.minimum_flex(start,
                                           flexibility_horizon_hours=SIMULATION_HOURS)
        flex_min = list(flex_min.to_dict()['p_kw'].values())
        start_dt = datetime.strptime(start, GER)

        flex = []
        for idx in range(len(flex_max)):
            # kw to mw
            flex.append([-flex_min[0] / 1000, -flex_max[0] / 1000])
            start_dt = start_dt + timedelta(minutes=15)
        if self._obligations is not None:
            for s_idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    flex[s_idx] = self._obligations[date]
        self._flex_for_date[start_date] = flex
        return flex


class WindAgent(UnitAgent):

    def __init__(self, container, suggested_aid, max_power=8, schedule_provider=None):
        super().__init__(container, suggested_aid)
        self._wind_system = WindPowerPlantSystem(*wind_presets(pn_max_kw=max_power))
        self._weather_data = pd.read_csv(WD_PATH, index_col=0)
        if schedule_provider is None:
            self.schedule_provider = self.schedule_provider
        else:
            self.schedule_provider = schedule_provider
        self._maximal_schedule_per_date = {}
        self._start_date = None

    def calculate_power_feed_in(self, date):
        if date != self._current_dates[0]:
            self.update_dates(date)
        now_dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        p_kws = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        q_kvars = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        cos_phis = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        winds = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)

        for i in range(SIMULATION_HOURS_IN_RESOLUTION):
            widx = datetime_to_index(now_dt)
            self._wind_system.set_step_size(STEP_SIZE)
            self._wind_system.set_now_dt(now_dt)
            self._wind_system.inputs.t_air_deg_celsius = self._weather_data.iloc[widx][T_AIR]
            self._wind_system.inputs.wind_v_m_per_s = self._weather_data.iloc[widx][WIND]
            self._wind_system.inputs.air_pressure_hpa = self._weather_data.iloc[widx][PRESSURE]

            self._wind_system.step()

            p_kws[i] = self._wind_system.get_p_kw()
            q_kvars[i] = self._wind_system.get_q_kvar()
            cos_phis[i] = self._wind_system.get_cos_phi()
            winds[i] = self._weather_data.iloc[widx][WIND]
            now_dt += timedelta(seconds=STEP_SIZE)

        if self._obligations is not None:
            for idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    p_kws[idx] = self._obligations[date]
        self._maximal_schedule_per_date[date] = p_kws
        return p_kws

    def schedule_provider(self, target_params):
        if target_params['current_start_date'] != self._current_dates[0]:
            self.update_dates(target_params['current_start_date'])
        if target_params['current_start_date'] in self._schedules_for_date.keys():
            return self._schedules_for_date[target_params['current_start_date']]
        if target_params['current_start_date'] in self._maximal_schedule_per_date.keys():
            p_kws = self._maximal_schedule_per_date[target_params['current_start_date']]
        else:
            p_kws = self.calculate_power_feed_in(target_params['current_start_date'])
        schedules = []
        for p in SCHEDULE_PERCENTAGES:
            schedule_p = [round((p * x), 15) for x in p_kws]
            if self._obligations is not None:
                for s_idx, date in enumerate(self._current_dates):
                    if date in self._obligations.keys():
                        schedule_p[s_idx] = self._obligations[date]
            schedules.append(schedule_p)
        self._schedules_for_date[target_params['current_start_date']] = schedules
        return schedules

    def calculate_penalty(self, target_params, chosen_schedule):
        maximal_schedule = self._maximal_schedule_per_date[target_params['current_start_date']]
        penalty = 0
        for i in range(len(maximal_schedule)):
            penalty += abs(maximal_schedule[i]) - abs(chosen_schedule[i])
        return penalty

    def calculate_redispatch_flexibility(self, start_date=None) -> List:
        if start_date != self._current_dates[0]:
            self.update_dates(start_date)
        if start_date not in self._maximal_schedule_per_date.keys():
            max_p_kws = self.calculate_power_feed_in(start_date)
        else:
            max_p_kws = self._maximal_schedule_per_date[start_date]
        min_p_kws = [0. for _ in range(len(max_p_kws))]
        if self._obligations is not None:
            for s_idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    max_p_kws[s_idx] = self._obligations[date]
        self._flex_for_date[start_date] = [[min_p_kws[i], max_p_kws[i]] for i in range(len(max_p_kws))]
        return self._flex_for_date[start_date]


class PVAgent(UnitAgent):

    def __init__(self, container, suggested_aid, peak_power=8, schedule_provider=None):
        super().__init__(container, suggested_aid)
        self._weather_data = pd.read_csv(WD_PATH, index_col=0)
        self._pv_system = PVPlantSystem(*pv_preset(p_peak_kw=peak_power, cos_phi=0.95))
        if schedule_provider is None:
            self.schedule_provider = self.schedule_provider
        else:
            self.schedule_provider = schedule_provider
        self._maximal_schedule_per_date = {}

    def schedule_provider(self, target_params):
        if target_params['current_start_date'] != self._current_dates[0]:
            self.update_dates(target_params['current_start_date'])
        if target_params['current_start_date'] in self._schedules_for_date.keys():
            return self._schedules_for_date[target_params['current_start_date']]
        if target_params['current_start_date'] in self._maximal_schedule_per_date.keys():
            p_kws = self._maximal_schedule_per_date[target_params['current_start_date']]
        else:
            p_kws = self.calculate_power_feed_in(target_params['current_start_date'])
        schedules = []
        for p in SCHEDULE_PERCENTAGES:
            schedule_p = [round((p * x), 15) for x in p_kws]
            if self._obligations is not None:
                for s_idx, date in enumerate(self._current_dates):
                    if date in self._obligations.keys():
                        schedule_p[s_idx] = self._obligations[date]
            schedules.append(schedule_p)
        self._schedules_for_date[target_params['current_start_date']] = schedules
        return schedules

    def calculate_penalty(self, target_params, chosen_schedule):
        maximal_schedule = self._maximal_schedule_per_date[target_params['current_start_date']]
        penalty = 0
        for i in range(len(maximal_schedule)):
            penalty += abs(maximal_schedule[i]) - abs(chosen_schedule[i])
        return penalty

    def calculate_power_feed_in(self, date):
        if date != self._current_dates[0]:
            self.update_dates(date)
        now_dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        p_kws = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        q_kvars = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        cos_phis = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)
        t_modules = np.zeros(SIMULATION_HOURS_IN_RESOLUTION)

        for i in range(SIMULATION_HOURS_IN_RESOLUTION):
            widx = datetime_to_index(now_dt)
            self._pv_system.set_step_size(STEP_SIZE)
            self._pv_system.set_now_dt(now_dt)
            self._pv_system.inputs.t_air_deg_celsius = self._weather_data.iloc[widx][T_AIR]
            self._pv_system.inputs.bh_w_per_m2 = self._weather_data.iloc[widx][BH]
            self._pv_system.inputs.dh_w_per_m2 = self._weather_data.iloc[widx][DH]

            self._pv_system.step()

            p_kws[i] = self._pv_system.get_p_kw()
            q_kvars[i] = self._pv_system.get_q_kvar()
            cos_phis[i] = self._pv_system.get_cos_phi()
            t_modules[i] = self._pv_system.state.t_module_deg_celsius

            now_dt += timedelta(seconds=STEP_SIZE)
        if self._obligations is not None:
            for idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    p_kws[idx] = self._obligations[date]
        self._maximal_schedule_per_date[date] = p_kws
        return p_kws

    def calculate_redispatch_flexibility(self, start_date=None) -> List:
        if start_date is not None and start_date != self._current_dates[0]:
            self.update_dates(start_date)
        if start_date in self._flex_for_date.keys():
            return self._flex_for_date[start_date]

        if start_date not in self._maximal_schedule_per_date.keys():
            max_p_kws = self.calculate_power_feed_in(start_date)
        else:
            max_p_kws = self._maximal_schedule_per_date[start_date]
        min_p_kws = [0. for _ in range(len(max_p_kws))]
        if self._obligations is not None:
            for s_idx, date in enumerate(self._current_dates):
                if date in self._obligations.keys():
                    max_p_kws[s_idx] = self._obligations[date]
        self._flex_for_date[start_date] = [[min_p_kws[i], max_p_kws[i]] for i in range(len(max_p_kws))]
        return self._flex_for_date[start_date]
