import asyncio
import os
import random
import time
import uuid
from enum import Enum

import h5py

from agents.centralized_observer import CentralizedObserverAgentRole
from agents.controller_agent import CentralizedControllerAgent
from mango_library.negotiation.winzent.util_functions import create_agents, shutdown

from os.path import abspath
from pathlib import Path
import pandas as pd

ROOT_PATH = Path(abspath(__file__)).parent
data_path = ROOT_PATH / 'data' / 'data_unit_control'


class ATTACK(Enum):
    NONE = 0
    NO_SIGNAL = 1
    ZERO_VALUES = 2


class CONTROL(Enum):
    NONE = 0
    CENTRALIZED = 1
    DECENTRALIZED = 2
    ML = 3


async def scenario():
    """
    """
    # load flexibility pv, wind, chp
    # 0 = no attack, 1 = no signal, 2 = zero values
    attack = ATTACK.NO_SIGNAL.value
    START = '2022-07-23 05:15:00Z'
    number_of_agents = 150
    ttl = 250#300  # 99
    optimizations = 50
    time_to_sleep = 250#350  # 250-350 # 35 # TODO
    control = CONTROL.DECENTRALIZED.value

    chp_schedules = {}

    config_wind = ['500', '700', '750', '750', '800', '900', '950', '1000']
    config_pv = ['200', '300', '400', '400', '500', '550', '600', '700', '800', '950']
    config_chp = ['400' for _ in range(int(10))]
    data_dict = {}
    chps = number_of_agents / 10
    left = number_of_agents - 10
    target_range = [0, 0]

    while len(config_wind) < int(left / 2):
        config_wind.append(config_wind[random.randint(0, len(config_wind) - 1)])

    while len(config_pv) < int(left / 2):
        config_pv.append(config_pv[random.randint(0, len(config_pv) - 1)])

    while len(config_chp) < int(chps):
        config_chp.append(config_chp[random.randint(0, len(config_chp) - 1)])

    for idx, kw in enumerate(config_pv):
        path = data_path / f'pv_power_{kw}_kw_{START}.csv'
        pv_data_csv = pd.read_csv(path)
        pv_forecast_data = pv_data_csv['kw'].tolist()
        data_dict[idx] = pv_forecast_data
    target_range[-1] += max(pv_forecast_data)

    for idx, kw in enumerate(config_wind, start=int(left / 2)):
        path = data_path / f'wind_power_{kw}_kw_{START}.csv'
        # read example power data for wind
        wind_data_csv = pd.read_csv(path)
        wind_forecast_data = wind_data_csv['kw'].tolist()
        wind_forecast_data = [i * 100000 for i in wind_forecast_data]
        data_dict[idx] = wind_forecast_data
    target_range[-1] += max(wind_forecast_data)

    ctr = 0
    for idx in range(int(left), (len(config_chp) + int(left))):
        if ctr > 9:
            ctr = 0
        path = data_path / f'chp_400_kw_{ctr}_{START}.csv'
        chp_data = pd.read_csv(path)
        chp_schedules[idx] = []
        ctr += 1
        for schedule_idx in range(1):
            schedule_entry = chp_data[str(schedule_idx)].tolist()
            # schedule_entry = [entry * -1 for entry in schedule_entry]
            data_dict[idx] = schedule_entry
    target_range[-1] += max(schedule_entry)

    target_range[-1] *= 1.5
    agents, container, agent_names = await create_agents(number_of_agents=number_of_agents, ttl=ttl,
                                                         time_to_sleep=time_to_sleep, attack=attack,
                                                         control=control)

    optimization_time = 0
    for t in range(optimizations):
        already_updated = []
        for a_idx, agent in enumerate(agents):
            flex = round(data_dict[a_idx][t])
            if flex == 0:
                flex = 2
            if a_idx in already_updated:
                continue
            if agent.flagged_malicious:
                if agent.flagged_malicious < a_idx:
                    agents[agent.flagged_malicious].flex[optimization_time][-1] += flex
                else:
                    already_updated.append(a_idx)
                    agents[agent.flagged_malicious].flex[optimization_time] = flex + round(
                        data_dict[agent.flagged_malicious][optimization_time])
            agent.update_flexibility(t_start=optimization_time, min_p=0, max_p=flex)

        low = target_range[1] #/ 2
        target_value = int(random.uniform(low, target_range[1]))
        print('target:', target_value)
        start_time = time.time()
        await agents[0].start_negotiation(ts=[optimization_time, optimization_time + 900], value=target_value)
        await agents[0].negotiation_done
        end_time = time.time()
        duration = end_time - start_time
        print(duration)

        db_file = 'simulation_results_' + str(optimization_time) + '.hdf5'
        with h5py.File(db_file, "w") as f:
            f.close()

        await store_msgs(db_file, optimization_time, agent_names, duration, target_value)

        optimization_time += 900

    await shutdown(agents, [container])


async def store_msgs(db_file, time, agent_names, duration, target_value):
    print('store final results')
    f = h5py.File(db_file, 'a')
    grp_name = f"negotiation+{time}"
    if grp_name not in f:
        print('try to store msgs but does not exist?')
        grp = f.create_group(grp_name)
    else:
        grp = f[grp_name]

    f = h5py.File(db_file, 'a')
    print('store results now')
    try:
        general_group = f.create_group(f'{time}')
    except ValueError:
        general_group = f.create_group(f'{time}_{str(uuid.uuid4())}')
    f.attrs['target'] = str(target_value)
    general_group.attrs['target'] = str(target_value)
    f.attrs['duration'] = str(duration)
    general_group.attrs['duration'] = str(duration)

    if os.path.isfile("agent0_msg.h5") or os.path.isfile("agent1_msg.h5") or os.path.isfile(
            "agent2_msg.h5") or os.path.isfile("agent3_msg.h5") or \
            os.path.isfile("agent4_msg.h5") or os.path.isfile("agent5_msg.h5") or os.path.isfile(
        "agent6_msg.h5") or os.path.isfile("agent7_msg.h5") or\
        os.path.isfile("agent8_msg.h5") or os.path.isfile("agent9_msg.h5") or os.path.isfile(
            "agent10_msg.h5") or os.path.isfile("agent11_msg.h5") or os.path.isfile("agent12_msg.h5") or os.path.isfile(
            "agent13_msg.h5") or os.path.isfile("agent14_msg.h5") or os.path.isfile("agent15_msg.h5") or os.path.isfile("agent16_msg.h5")\
            or os.path.isfile("agent17_msg.h5") or os.path.isfile("agent18_msg.h5")or os.path.isfile("agent19_msg.h5") or os.path.isfile("agent20_msg.h5")\
            or os.path.isfile("agent21_msg.h5")or os.path.isfile("agent22_msg.h5")or os.path.isfile("agent23_msg.h5")or os.path.isfile("agent24_msg.h5")\
            or os.path.isfile("agent25_msg.h5")or os.path.isfile("agent26_msg.h5")or os.path.isfile("agent27_msg.h5")or os.path.isfile("agent28_msg.h5")\
            or os.path.isfile("agent29_msg.h5")or os.path.isfile("agent30_msg.h5")or os.path.isfile("agent31_msg.h5")\
            or os.path.isfile("agent32_msg.h5")or os.path.isfile("agent33_msg.h5")or os.path.isfile("agent34_msg.h5")or os.path.isfile("agent35_msg.h5")\
            or os.path.isfile("agent36_msg.h5")or os.path.isfile("agent37_msg.h5")or os.path.isfile("agent38_msg.h5")or os.path.isfile("agent39_msg.h5")\
            or os.path.isfile("agent40_msg.h5")or os.path.isfile("agent41_msg.h5")or os.path.isfile("agent42_msg.h5")or os.path.isfile("agent43_msg.h5")or os.path.isfile("agent44_msg.h5")\
            or os.path.isfile("agent45_msg.h5")or os.path.isfile("agent46_msg.h5")or os.path.isfile("agent47_msg.h5")or os.path.isfile("agent48_msg.h5")or os.path.isfile("agent49_msg.h5")\
            or os.path.isfile("agent50_msg.h5")or os.path.isfile("agent51_msg.h5")or os.path.isfile("agent52_msg.h5")or os.path.isfile("agent53_msg.h5")or os.path.isfile("agent54_msg.h5")or os.path.isfile("agent55_msg.h5")or os.path.isfile("agent56_msg.h5")or os.path.isfile("agent57_msg.h5")\
            or os.path.isfile("agent58_msg.h5")\
            or os.path.isfile("agent59_msg.h5") or os.path.isfile("agent60_msg.h5") or os.path.isfile("agent61_msg.h5") or os.path.isfile("agent62_msg.h5") or os.path.isfile("agent63_msg.h5")\
            or os.path.isfile("agent64_msg.h5")or os.path.isfile("agent65_msg.h5")or os.path.isfile("agent66_msg.h5")or os.path.isfile("agent67_msg.h5")or os.path.isfile("agent68_msg.h5")\
            or os.path.isfile("agent69_msg.h5")or os.path.isfile("agent70_msg.h5")or os.path.isfile("agent71_msg.h5")or os.path.isfile("agent72_msg.h5")or os.path.isfile("agent73_msg.h5")\
            or os.path.isfile("agent74_msg.h5")or os.path.isfile("agent75_msg.h5")or os.path.isfile("agent76_msg.h5")or os.path.isfile("agent77_msg.h5")or os.path.isfile("agent78_msg.h5")\
            or os.path.isfile("agent79_msg.h5")or os.path.isfile("agent80_msg.h5")or os.path.isfile("agent81_msg.h5")or os.path.isfile("agent82_msg.h5")or os.path.isfile("agent83_msg.h5")\
            or os.path.isfile("agent84_msg.h5")or os.path.isfile("agent85_msg.h5")or os.path.isfile("agent86_msg.h5")or os.path.isfile("agent87_msg.h5") or os.path.isfile("agent88_msg.h5")\
            or os.path.isfile("agent89_msg.h5") or os.path.isfile("agent90_msg.h5") or os.path.isfile("agent91_msg.h5") or os.path.isfile("agent92_msg.h5") or os.path.isfile("agent93_msg.h5") or os.path.isfile("agent94_msg.h5")\
            or os.path.isfile("agent95_msg.h5")or os.path.isfile("agent96_msg.h5")or os.path.isfile("agent97_msg.h5")or os.path.isfile("agent98_msg.h5")or os.path.isfile("agent99_msg.h5"):
        # open updates from agents, if those are stored

        agent_msgs = [f"{agent_name}_msg.h5" for agent_name in agent_names]
        for c_agent_name in agent_msgs:
            file = h5py.File(c_agent_name, "a")
            if os.path.isfile(c_agent_name):
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            os.remove(c_agent_name)

    if os.path.isfile("agent100_msg.h5"):
        # open updates from agents, if those are stored
        agent_msgs = ["agent100_msg"]
        for c_agent_name in agent_msgs:
            file = h5py.File(c_agent_name, "a")
            if os.path.isfile(c_agent_name):
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            os.remove(c_agent_name)

    if os.path.isfile("agent101_msg.h5"):
        # open updates from agents, if those are stored
        agent_msgs = ["agent101_msg"]
        for c_agent_name in agent_msgs:
            file = h5py.File(c_agent_name, "a")
            if os.path.isfile(c_agent_name):
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            os.remove(c_agent_name)

    if os.path.isfile("agent102_msg.h5"):
        # open updates from agents, if those are stored
        agent_msgs = ["agent102_msg"]
        for c_agent_name in agent_msgs:
            file = h5py.File(c_agent_name, "a")
            if os.path.isfile(c_agent_name):
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            os.remove(c_agent_name)

    if os.path.isfile("agent0_rec_msg.h5"):
        # open updates from agents, if those are stored
        agent_msgs = [f"{agent_name}_rec_msg.h5" for agent_name in agent_names]
        for c_agent_name in agent_msgs:
            file = h5py.File(c_agent_name, "a")
            if os.path.isfile(c_agent_name):
                groups = [key for key in file.keys()]
                groups.sort()
                for key in groups:
                    file.copy(key, grp, name=f"{c_agent_name[:-2]}_{key}")
            os.remove(c_agent_name)

    if os.path.isfile("balance_agent0.hdf5"):
        # open updates from agents, if those are stored
        file = h5py.File("balance_agent0.hdf5", "a")
        groups = [key for key in file.keys()]
        groups.sort()
        for key in groups:
            file.copy(key, grp, name=f"balance_agent0")
        os.remove("balance_agent0.hdf5")
    f.close()


def main():
    asyncio.run(scenario())


if __name__ == '__main__':
    main()
