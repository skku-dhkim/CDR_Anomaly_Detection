import pandas as pd
import pickle
import os
import dill
import glob
import config.pgw_ip_address as pgw_ip
from models.rrcf_cls import RRCF
from inspect import getframeinfo, stack


def _debug_info(message, m_type='INFO'):
    insp = getframeinfo(stack()[1][0])
    if m_type == "INFO":
        print("[*] INFO:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "WARNING":
        print("[@] WARNING:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "ERROR":
        print("[!] ERROR:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
        raise SystemExit()
    else:
        raise ValueError("Invalid input vlaue \'m_type\': {}".format(m_type))


def data_separation(pgw_ip, svc_type):
    '''
    Data load from csv file.
    '''
    df = pd.read_csv("./data/{}/{}.csv".format(pgw_ip, svc_type),
                     names=['DTmm', 'Real_Up', 'Real_Dn', 'Free_Up', 'Free_Dn'])
    df['DTmm'] = pd.to_datetime(df['DTmm'], format='%Y-%m-%d %H:%M')
    df_train = df.loc[df['DTmm'] < '2019-08-01']
    df_test = df.loc[df['DTmm'] >= '2019-08-01'].reset_index(drop=True)

    return df_train, df_test


def train_models(data, num_of_trees, sequences, num_of_leaves, write_file=False):
    date = data['data']['DTmm']
    train_data = data['data'][['Real_Up', 'Real_Dn', 'Free_Up', 'Free_Dn']]
    train_data = train_data.to_numpy()
    o_rrcf = RRCF(num_trees=num_of_trees, sequences=sequences, leaves_size=num_of_leaves)
    score, ftime = o_rrcf.train_rrcf(date, train_data, timer=True)
    _debug_info("Required time: {}".format(ftime))

    _ = o_rrcf.calc_threshold(score, QUANTILE, with_data=False)
    _debug_info("Threshold: {}".format(o_rrcf.threshold))

    if write_file:
        instance_path = "./instances/{}/{}/".format(data['pgw_ip'], data['svc_type'])

        if not os.path.exists(instance_path):
            _debug_info("Instance directory is not exist. Creating one...")
            os.makedirs(instance_path)

        with open(instance_path + "hyper_parameter.txt", "w") as file:
            file.write("number of trees: {}\n".format(o_rrcf.num_trees))
            file.write("number of leaves: {}\n".format(o_rrcf.num_trees))
            file.write("sequences: {}\n".format(o_rrcf.sequences))
            file.write("required time: {}\n".format(ftime))

        with open(instance_path + "model.pkl", "wb") as output:
            dill.dump(o_rrcf, output)

        with open(instance_path + "anomaly_scores.dict", "wb") as file:
            dill.dump(score, file)

    return score, o_rrcf


def load(pgw_ip, svc_type):
    with open('./instances/{}/{}/model.pkl'.format(pgw_ip, svc_type), "rb") as file:
        rrcf_object = pickle.load(file)
    return rrcf_object


'''
Hyper parameters for RRCF algorithm.
'''
NUM_OF_TREES = 80
NUM_OF_LEAVES = 864
QUANTILE = 0.99
SEQUENCES = 6

l_pgw_ip = pgw_ip.l_pgw_ip

for pgw_ip in l_pgw_ip:

    _debug_info("Running \'pgw_ip - {}\'".format(pgw_ip))
    l_svc_type = glob.glob("./data/{}/*.csv".format(pgw_ip))

    for fname in l_svc_type:

        name = fname.split('/')[-1]
        svc_type = name.split('.')[0]

        _debug_info("\t \'svc_type - {}\'".format(svc_type))

        df_train, df_test = data_separation(pgw_ip, svc_type)

        if len(df_train.index) < SEQUENCES:
            continue

        data = {
            'pgw_ip': pgw_ip,
            'svc_type': svc_type,
            'data': df_train
        }

        try:
            s, rrcf = train_models(data, NUM_OF_TREES, SEQUENCES, NUM_OF_LEAVES, write_file=True)
        except Exception as e:
            _debug_info("PGW IP: {} / SVC_TYPE: {} / Error occurs: {}".format(pgw_ip, svc_type, e))
            continue
