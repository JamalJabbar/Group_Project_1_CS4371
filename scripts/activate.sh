# Source this file from Bash: source scripts/activate.sh
_cs4371_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "$_cs4371_root/.venv/bin/activate"
export JAVA_HOME="$(dirname -- "$(dirname -- "$(readlink -f -- "$(command -v java)")")")"
export SPARK_HOME="$(python -c 'from pathlib import Path; import pyspark; print(Path(pyspark.__file__).parent)')"
export SPARK_CONF_DIR="$_cs4371_root/config/spark"
export PYSPARK_PYTHON="$_cs4371_root/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} -Divy.home=$_cs4371_root/.cache/ivy"
export HF_HOME="$_cs4371_root/.cache/huggingface"
export PIP_CACHE_DIR="$_cs4371_root/.cache/pip"
export IPYTHONDIR="$_cs4371_root/.cache/ipython"
export JUPYTER_CONFIG_DIR="$_cs4371_root/.cache/jupyter/config"
export JUPYTER_DATA_DIR="$_cs4371_root/.venv/share/jupyter"
export JUPYTER_RUNTIME_DIR="$_cs4371_root/.cache/jupyter/runtime"
unset _cs4371_root
