def svc_log_dir(PGW_IP, SVC_TYPE):
    return './management/{}/{}/log/'.format(PGW_IP, SVC_TYPE)


def output_dir(PGW_IP, SVC_TYPE):
    return './management/{}/{}/output/'.format(PGW_IP, SVC_TYPE)


def input_dir(PGW_IP, SVC_TYPE):
    return './management/{}/{}/input/'.format(PGW_IP, SVC_TYPE)


def complete_dir():
    return './COMPLETE/'


def original_input_path():
    return './INPUT/'


def log_dir():
    return './management/log/'


def anomaly_score_dir(PGW_IP, SVC_TYPE):
    return "./management/{}/{}/anomaly_scores/".format(PGW_IP, SVC_TYPE)
