
from app.common.logging import Console
from app.session import config
from typing import List
from app import Task

import argparse
import logging
import json
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
        help="The interval in seconds to run the task"
    )
    parser.add_argument(
        "-l", "--list",
        action="store_true",
        help="List all available tasks"
    )
    parser.add_argument(
        "-f", "--file",
        type=str,
        help="Use a schedule config file to run multiple tasks"
    )
    parser.add_argument(
        'task_arguments',
        nargs=argparse.REMAINDER
    )

    return vars(parser.parse_args())

def tasks_from_file(file: str) -> List[Task]:
    task_names = [task.__name__ for task in app.TASKS]

    with open(file, 'r') as f:
        tasks = json.load(f)

    return [
        Task(
            app.TASKS[task_names.index(task['name'])],
            task['interval'],
            task['args']
        )
        for task in tasks
    ]

def main():
    args = parse_arguments()
    task_names = [task.__name__ for task in app.TASKS]
    interval = args["interval"]

    if args["list"]:
        app.session.logger.info("Available tasks:")
        for task in task_names:
            app.session.logger.info(f"  - {task}")
        return

    if args["file"]:
        app.run_task_loop(tasks_from_file(args["file"]))
        return

    if not args["name"]:
        app.session.logger.error("No task name provided.")
        return

    if args["name"] not in task_names:
        app.session.logger.warning(f"Task '{args['name']}' not found.")
        return

    if not interval:
        return Task(
            app.TASKS[task_names.index(args["name"])],
            args=args["task_arguments"]
        ).run()

    return app.run_task_loop([
        Task(
            app.TASKS[task_names.index(args["name"])],
            interval,
            args["task_arguments"]
        )
    ])

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        app.session.logger.info("Shutting down...")
    finally:
        app.session.database.engine.dispose()
        app.session.redis.close()
