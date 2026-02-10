# Read the doc: https://huggingface.co/docs/hub/spaces-sdks-docker
# you will also find guides on how best to write your Dockerfile

FROM python:3.12

RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /app

# cache requirements.txt
COPY --chown=user ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

RUN curl -L "https://github.com/waxz/iotcore/releases/download/v3.0.4/iotcore-3.0.4-cp312-cp312-manylinux_2_38_x86_64.whl" -o /app/iotcore-3.0.4-cp312-cp312-manylinux_2_38_x86_64.whl
RUN pip install /app/iotcore-3.0.4-cp312-cp312-manylinux_2_38_x86_64.whl

COPY --chown=user . /app

RUN python /app/pyx/setup.py build_ext -b /app/


CMD ["python", "run_broker.py"]
