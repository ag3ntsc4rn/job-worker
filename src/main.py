"""Script entrypoint: `cd src && python main.py`.

Running a script puts its own directory on ``sys.path``, so from here the
``worker`` package next door is importable without an install or ``PYTHONPATH``.
From the repo root use ``python -m worker`` instead.
"""

from worker.__main__ import main

if __name__ == "__main__":
    main()
