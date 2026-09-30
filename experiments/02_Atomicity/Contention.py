import json
import threading
from pathlib import Path


TABLE_DIR = Path("data/generated/02_table/contention")
METADATA_FILE = TABLE_DIR / "metadata.json"

commit_lock = threading.Lock()

# Shared metrics
metrics_lock = threading.Lock()

conflicts = 0
commits = 0
attempts = 0


def read_state():
    with open(METADATA_FILE) as f:
        return json.load(f)


def write_state(state):
    with open(METADATA_FILE, "w") as f:
        json.dump(state, f)


def setup():
    TABLE_DIR.mkdir(parents=True, exist_ok=True)

    write_state({
        "version": 1,
        "value": 0,
    })


def increment(worker_name, increments):
    global conflicts
    global commits
    global attempts

    for _ in range(increments):

        while True:

            # -------------------------
            # READ
            # -------------------------
            state = read_state()

            observed_version = state["version"]
            observed_value = state["value"]

            # -------------------------
            # COMPUTE
            # -------------------------
            new_value = observed_value + 1

            # -------------------------
            # COMMIT
            # -------------------------
            with commit_lock:

                attempts += 1

                current_state = read_state()
                current_version = current_state["version"]

                # -------------------------
                # OCC VALIDATION
                # -------------------------
                if current_version != observed_version:

                    with metrics_lock:
                        conflicts += 1

                    continue

                # -------------------------
                # SUCCESS
                # -------------------------
                new_state = {
                    "version": observed_version + 1,
                    "value": new_value,
                }

                write_state(new_state)

                with metrics_lock:
                    commits += 1

                break


def run_experiment(num_workers, increments_per_worker):

    global conflicts
    global commits
    global attempts

    conflicts = 0
    commits = 0
    attempts = 0

    setup()

    threads = []

    for i in range(num_workers):

        thread = threading.Thread(
            target=increment,
            args=(
                f"Worker-{i}",
                increments_per_worker,
            ),
        )

        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    final_state = read_state()

    print("\n==============================")
    print(f"Workers: {num_workers}")
    print(f"Increments/worker: {increments_per_worker}")
    print("==============================")

    print(
        f"Expected value: "
        f"{num_workers * increments_per_worker}"
    )

    print(
        f"Actual value:   "
        f"{final_state['value']}"
    )

    print(f"Commits:         {commits}")
    print(f"Conflicts:       {conflicts}")
    print(f"Attempts:        {attempts}")

    print(
        f"Average attempts "
        f"per successful increment: "
        f"{attempts / commits:.2f}"
    )


def main():

    # Start small.
    run_experiment(
        num_workers=2,
        increments_per_worker=100,
    )
    run_experiment(
        num_workers=10,
        increments_per_worker=100,
    )


if __name__ == "__main__":
    main()