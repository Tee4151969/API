FROM python:3
WORKDIR /usr/src/app
COPY Log.csv /usr/src/app/Log.csv
COPY Masters.csv /usr/src/app/Masters.csv
COPY requirements.txt /usr/src/app/requirements.txt
COPY main.py /usr/src/app/main.py
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python","./main.py"]