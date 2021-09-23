# Start from a core stack version
FROM jupyter/datascience-notebook:33add21fab64

# Install from requirements.txt file
COPY --chown=${NB_UID}:${NB_GID} requirements_dev.txt /tmp/requirements.txt
RUN pip install --quiet --no-cache-dir --requirement /tmp/requirements.txt && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

ENTRYPOINT ["jupyter", "notebook", "--NotebookApp.iopub_data_rate_limit=1e10", "--ip=0.0.0.0", "--allow-root"]
