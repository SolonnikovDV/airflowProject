import os
import config as cfg


def start_yc_vm():
    os.system(f'yc compute instance start {cfg.AIRFLOW_VM}')


def connect_to_yc_vm():
    public_ip = str(os.popen('curl http://169.254.169.254/latest/meta-data/public-ipv4'))
    print(str(public_ip))
    os.system(f'ssh {cfg.VM_USER_NAME}@{public_ip}')
    # TODO make request of meta-data VM as test of successful connection


def stop_yc():
    os.system(f'yc compute instance stop {cfg.AIRFLOW_VM}')
    pass


if __name__ == '__main__':
    print('')
    start_yc_vm()
    # connect_to_yc_vm()
    # stop_yc()