def management_dir():
    return "./management"


def svc_log_dir(PGW_IP, SVC_TYPE):
    return '{}/{}/{}/log/'.format(management_dir(), PGW_IP, SVC_TYPE)


def output_dir(PGW_IP, SVC_TYPE):
    return '{}/{}/{}/output/'.format(management_dir(), PGW_IP, SVC_TYPE)


def input_dir(PGW_IP, SVC_TYPE):
    return '{}/{}/{}/input/'.format(management_dir(), PGW_IP, SVC_TYPE)


def complete_dir():
    return './COMPLETE/'


def original_input_path():
    return './INPUT/'


def final_output_path():
    return "./OUTPUT/"


def log_dir():
    return '{}/log/'.format(management_dir())


def anomaly_score_dir(PGW_IP, SVC_TYPE):
    return "{}/{}/{}/anomaly_scores/".format(management_dir(), PGW_IP, SVC_TYPE)


def run_dir():
    return '{}/running/'.format(management_dir())


def backup_dir():
    # return '/OPER/ML/SVCTYPE/BACKUP/'
    return './BACKUP/'
