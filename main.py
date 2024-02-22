
from app.common.logging import Console
import argparse
import logging
import config
import app

logging.basicConfig(
    format='[%(asctime)s] - <%(name)s> %(levelname)s: %(message)s',
    level=logging.DEBUG if config.DEBUG else logging.INFO,
    handlers=[Console]
)

def parse_arguments() -> dict:
    parser = argparse.ArgumentParser(
        prog="jobs",
        description="A collection of background-tasks for titanic"
    )

    parser.add_argument(
        "-n", "--name",
        type=str,
        help="The name of the task to run"
    )
    parser.add_argument(
        "-i", "--interval",
        type=int,
        help="The interval in seconds to run the tasks",
        default=60
    )
    parser.add_argument(
        "-o", "--once",
        action="store_true",
        help="Run the task once and then exit"
    )

    return vars(parser.parse_args())

def main():
    args = parse_arguments()
    task_names = [task.__name__ for task in app.TASKS]
    interval = args["interval"]

    if args["name"]:
        if args["name"] not in task_names:
            app.session.logger.warning(f"Task '{args['name']}' not found.")
            return

        if args["once"]:
            app.run_tasks([app.TASKS[task_names.index(args["name"])]])
            return

        app.run_task_loop([app.TASKS[task_names.index(args["name"])]], interval)
        return

    if args["once"]:
        app.run_tasks(app.TASKS)
        return

    app.run_task_loop(app.TASKS, interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

    app.session.logger.warning("Exiting...")
    exit(0)
