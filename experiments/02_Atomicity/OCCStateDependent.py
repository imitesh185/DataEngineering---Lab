import json
import threading
from pathlib import Path


TABLE_DIR = Path("data/generated/02_table/OCCStateDependentOrders")
METADATA_FILE = TABLE_DIR / "metadata.json"

commit_lock = threading.Lock()
barrier = threading.Barrier(2)


def read_state():
    with open(METADATA_FILE) as f:
        return json.load(f)


def write_state(state):
    with open(METADATA_FILE, "w") as f:
        json.dump(state, f, indent=2)


def setup():
    TABLE_DIR.mkdir(parents=True, exist_ok=True)

    write_state({
        "version": 1,
        "value": 1
    })


def increment(name):
    # --------------------------------
    # 1. READ
    # --------------------------------
    state = read_state()

    observed_version = state["version"]
    observed_value = state["value"]

    print(
        f"{name}: read V{observed_version}, "
        f"value={observed_value}"
    )

    # --------------------------------
    # 2. PERFORM OPERATION
    # --------------------------------
    new_value = observed_value + 1

    print(
        f"{name}: calculated "
        f"{observed_value} + 1 = {new_value}"
    )

    # Force both writers to calculate
    # from V1 before either commits.
    barrier.wait()

    # --------------------------------
    # 3. TRY COMMIT
    # --------------------------------
    with commit_lock:

        current_state = read_state()
        current_version = current_state["version"]

        # --------------------------------
        # 4. VALIDATE
        # --------------------------------
        if current_version != observed_version:

            print(
                f"{name}: CONFLICT!"
            )

            print(
                f"{name}: expected V{observed_version}, "
                f"found V{current_version}"
            )

            # --------------------------------
            # 5. RE-READ NEW STATE
            # --------------------------------
            observed_version = current_state["version"]
            observed_value = current_state["value"]

            print(
                f"{name}: retrying from "
                f"V{observed_version}, "
                f"value={observed_value}"
            )

            # --------------------------------
            # 6. RE-EXECUTE OPERATION
            # --------------------------------
            new_value = observed_value + 1

            print(
                f"{name}: recalculated "
                f"{observed_value} + 1 = {new_value}"
            )

        # --------------------------------
        # 7. COMMIT
        # --------------------------------
        new_state = {
            "version": observed_version + 1,
            "value": new_value
        }

        write_state(new_state)

        print(
            f"{name}: COMMITTED "
            f"V{new_state['version']}, "
            f"value={new_state['value']}"
        )


def main():
    setup()

    print("Initial state:")
    print(json.dumps(read_state(), indent=2))

    thread_a = threading.Thread(
        target=increment,
        args=("Writer A",)
    )

    thread_b = threading.Thread(
        target=increment,
        args=("Writer B",)
    )

    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()

    print("\nFinal state:")
    print(json.dumps(read_state(), indent=2))


if __name__ == "__main__":
    main()