import sys

import find_zero_topics
import main as topic_usage_main
import update_topic_owner


def main() -> None:
    args = sys.argv[1:]

    if args and args[0] in {"find-zero-topics", "zero-topics"}:
        # Forward remaining args to the zero-topics tool.
        sys.argv = ["find_zero_topics.py", *args[1:]]
        find_zero_topics.main()
        return

    if args and args[0] == "update-topic-owner":
        # Forward remaining args to the update-topic-owner tool.
        sys.argv = ["update_topic_owner.py", *args[1:]]
        update_topic_owner.main()
        return

    # Default behavior: run topic usage exporter.
    sys.argv = ["main.py", *args]
    topic_usage_main.main()


if __name__ == "__main__":
    main()
