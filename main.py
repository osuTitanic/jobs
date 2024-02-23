
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
    parser.add_argument(
        "-l", "--list",
        action="store_true",
        help="List all available tasks"
    )
    parser.add_argument(
        'task_arguments',
        nargs=argparse.REMAINDER
    )

    return vars(parser.parse_args())

def main():
    args = parse_arguments()
    task_names = [task.__name__ for task in app.TASKS]
    interval = args["interval"]

    if args["list"]:
        app.session.logger.info("Available tasks:")
        for task in task_names:
            app.session.logger.info(f"  - {task}")
        return

    if not args["name"]:
        app.session.logger.error("No task name provided.")
        return

    if args["name"] not in task_names:
        app.session.logger.warning(f"Task '{args['name']}' not found.")
        return

    if args["once"]:
        return app.run_tasks(
            [app.TASKS[task_names.index(args["name"])]],
            *args["task_arguments"]
        )

    return app.run_task_loop(
        [app.TASKS[task_names.index(args["name"])]],
        interval,
        *args["task_arguments"]
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(0)
