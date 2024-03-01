import parsl
import argparse
import time

from parsl.config import Config
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl import python_app


@python_app
def platform():
    import platform
    return platform.uname()


def priming():
    future = platform()
    print("Launched task.. waiting")
    print(f"Result : {future.result()}")

    
@python_app
def sleeper(sleep_dur=0, input_data="", output_data_volume:int=1):
    import time
    time.sleep(sleep_dur)
    return b'0' * output_data_volume



def test_sequence(num_workers, task_count=1, sleep_dur=0, input_data=0, output_data=0):

    prefix = f"[{num_workers=}][{task_count=}][{sleep_dur=}][{input_data=}][{output_data=}]"

    start = time.time()
    input_string = b'0' * input_data
    futures = [sleeper(0, input_string, output_data) for i in range(task_count)]
    launch_done = time.time() - start
    [future.result() for future in futures]
    exec_done = time.time() - start

    print(prefix + f"Launched tasks in {launch_done:.3f}s")
    print(prefix + f"Finished noop tasks in {exec_done:.3f}s")
    throughput = task_count / exec_done
    print(prefix + f"Throughput  {throughput:.3f} Tasks/s")
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--nodes", default="1",
                        help="Count of apps to launch")
    parser.add_argument("-w", "--workers_per_node", default="1",
                        help="Count of apps to launch")
    parser.add_argument("-c", "--count", default='',
                        help="Number of tasks to launch")

    args = parser.parse_args()

    config = Config(
    executors=[
        HighThroughputExecutor(
            label="Expanse",
            # worker_logdir_root='YOUR_LOGDIR_ON_COMET',
            max_workers=int(args.workers_per_node),
            prefetch_capacity=100,
            provider=SlurmProvider(
                'compute',
                # 'debug',
                # account="anl113",
                account="chi150",
                launcher=SrunLauncher(),
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options='',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='source ~/setup_parsl_test_env.sh',
                walltime='00:30:00',
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=int(args.nodes),
            ),
        )
    ]
    )

    parsl.load(config)
    priming()

    if not args.count:
        count_range = [100, 200]
    else:
        count_range = [int(args.count)]
        
    for data_volume in [0, 4, 8]:
        for task_count in count_range:
            for sleep_dur in [0, 1]:
                test_sequence(num_workers=int(args.nodes) * int(args.workers_per_node),
                              task_count=task_count,
                              sleep_dur=sleep_dur,
                              input_data=data_volume * 10**6,
                              output_data=data_volume * 10**6)
