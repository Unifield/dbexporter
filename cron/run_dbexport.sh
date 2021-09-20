#!/bin/sh
set -eu

usage() {
cat <<-'EOF'
Usage: run_dbexport.sh [-b BRANCH] [--] [ARGS]...
       run_dbexport.sh -h

Runs dbexporter. Any arguments provided are forwarded to the exporter.py.

Options:
  -b BRANCH  Name of dbexporter git branch to use for run,
             default: master
  -h         Show this help message
EOF
}

BRANCH=master
DBEXPORTER=/home/dbexporter/dbexporter
VENV=/home/dbexporter/venv

# Option reading
while getopts 'b:h' OPTION; do
        case "$OPTION" in
                b)
                        BRANCH="$OPTARG"
                        ;;
                h)
                        usage
                        exit 1
                        ;;
                *)
                  usage >&2
                  exit 1
                  ;;

        esac
done
shift $((OPTIND - 1))


cd "$DBEXPORTER"

# Get the newest version of dbexporter
git fetch
git checkout -- .
git checkout "$BRANCH" --
git pull

# Remove old venv and create new
rm -rf "$VENV"
python3.9 -m virtualenv "$VENV"
. "$VENV/bin/activate"
python3.9 -m pip install -r "$DBEXPORTER/requirements.txt"

# Export Python path
export PYTHONPATH="${PYTHONPATH}:$DBEXPORTER"

# Run exporter
python3.9 db_exporter/exporter.py "$@"