import parsl
import argparse
import time

from parsl.config import Config
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider, LocalProvider
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
    input_string = b'0' * input_data * 10**6
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
    parser.add_argument("-c", "--count", default="1",
                        help="Number of tasks to launch")

    args = parser.parse_args()

    config = Config(
    executors=[
        HighThroughputExecutor(
            label="Expanse",
            max_workers=int(args.workers_per_node),
            prefetch_capacity=100,
            provider=LocalProvider(),
        )
    ]
    )

    parsl.load(config)
    priming()

    for data_volume in [0, 4, 8, 20, 50]:
        for task_count in [100, 200]:
            for sleep_dur in [0, 1]:
                test_sequence(num_workers=int(args.nodes) * int(args.workers_per_node),
                              task_count=task_count,
                              sleep_dur=sleep_dur,
                              input_data=data_volume,
                              output_data=data_volume)
