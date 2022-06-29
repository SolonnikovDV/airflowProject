import os
import config as cfg


def start_yc_vm():
    os.system('yc compute instance start airflow-otus')


def connect_to_yc_vm():
    public_ip = os.system('curl http://169.254.169.254/latest/meta-data/public-ipv4')
    os.system(f'ssh {cfg.VM_USER_NAME}@{public_ip}')
    # TODO make request of meta-data VM as test of successful connection


def stop_yc():
    os.system('yc compute instance stop airflow-otus')
    # TODO find yc lib for py
    pass


if __name__ == '__main__':
    print('')
    start_yc_vm()
    print(os.system('yc compute instance start airflow-otus'))
    # connect_to_yc_vm()
    # stop_yc()